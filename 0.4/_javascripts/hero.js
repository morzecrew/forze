// Forze hero — a honeycomb lattice (hexagonal architecture) with a slow glow
// ripple travelling across it, zoned by a centre-left vignette. Pure canvas.
// Optimised: ~30fps, no shadow/mask layers, pauses when off-screen, stills for
// prefers-reduced-motion. Colour + intensity come from CSS variables.
(function () {
  var raf = null;
  var io = null;
  var schemeObs = null;

  function cssVar(name, fallback) {
    var v = getComputedStyle(document.body).getPropertyValue(name).trim();
    return v || fallback;
  }
  function readColor() {
    return cssVar("--forze-hero-color", "") || cssVar("--md-accent-fg-color", "#ff5722");
  }
  function readBoost() {
    return parseFloat(cssVar("--forze-hero-boost", "1")) || 1;
  }

  function start() {
    var canvas = document.getElementById("forze-hero-canvas");
    if (!canvas) return;

    if (raf) cancelAnimationFrame(raf), (raf = null);
    if (io) io.disconnect(), (io = null);
    if (schemeObs) schemeObs.disconnect(), (schemeObs = null);

    var ctx = canvas.getContext("2d");
    var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var color = readColor();
    var boost = readBoost();

    var w = 0,
      h = 0,
      hexes = [],
      verts = [],
      last = 0,
      visible = true;

    function seed() {
      var R = Math.max(30, Math.min(w, h) / 11); // hex radius
      var dx = Math.sqrt(3) * R; // pointy-top column spacing
      var dy = 1.5 * R; // row spacing
      verts = [];
      for (var k = 0; k < 6; k++) {
        var ang = (Math.PI / 180) * (60 * k - 90);
        verts.push([Math.cos(ang) * R, Math.sin(ang) * R]);
      }
      hexes = [];
      var row = 0;
      for (var y = -dy; y < h + dy; y += dy, row++) {
        var off = row % 2 ? dx / 2 : 0;
        for (var x = -dx; x < w + dx; x += dx) {
          // jitter position + phase so the lattice reads organic, not rigid
          var px = x + off + (Math.random() - 0.5) * R * 0.34;
          var py = y + (Math.random() - 0.5) * R * 0.34;
          // zone: bright centre-left, fading to the edges (hides pad seams)
          // and fading harder on the right
          var ex = px / w - 0.42;
          var ey = py / h - 0.5;
          var d = Math.sqrt(ex * ex * 1.3 + ey * ey * 2.2);
          var f = Math.max(0, Math.min(1, 1.22 - d * 1.85));
          f = f * f * (3 - 2 * f); // smoothstep falloff
          hexes.push({ x: px, y: py, ph: px * 0.011 + py * 0.014 + Math.random() * 0.6, f: f });
        }
      }
    }

    function resize() {
      var rect = canvas.getBoundingClientRect();
      if (!rect.width || !rect.height) return false;
      var dpr = Math.min(window.devicePixelRatio || 1, 1.5);
      w = rect.width;
      h = rect.height;
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      seed();
      return true;
    }

    function draw(t) {
      ctx.clearRect(0, 0, w, h);
      ctx.strokeStyle = color;
      ctx.lineWidth = boost > 1.3 ? 1.15 : 1;
      for (var i = 0; i < hexes.length; i++) {
        var hx = hexes[i];
        if (hx.f < 0.02) continue;
        var s = 0.5 + 0.5 * Math.sin(t - hx.ph); // 0..1 travelling wave
        var a = (0.04 + 0.5 * s * s * s) * hx.f * boost;
        ctx.globalAlpha = a > 1 ? 1 : a;
        ctx.beginPath();
        ctx.moveTo(hx.x + verts[0][0], hx.y + verts[0][1]);
        for (var k = 1; k < 6; k++) ctx.lineTo(hx.x + verts[k][0], hx.y + verts[k][1]);
        ctx.closePath();
        ctx.stroke();
      }
      ctx.globalAlpha = 1;
    }

    function loop(now) {
      if (!visible) {
        raf = null;
        return;
      }
      if (now - last >= 33) {
        // throttle to ~30fps
        last = now;
        draw(now * 0.00055); // slow ripple (~11s period)
      }
      raf = requestAnimationFrame(loop);
    }

    if (!resize()) {
      setTimeout(start, 120);
      return;
    }
    window.removeEventListener("resize", resize);
    window.addEventListener("resize", resize);

    // Re-read colour + intensity when the palette (light/dark) toggles.
    schemeObs = new MutationObserver(function () {
      color = readColor();
      boost = readBoost();
    });
    schemeObs.observe(document.body, {
      attributes: true,
      attributeFilter: ["data-md-color-scheme"],
    });

    if (reduce) {
      draw(0.7); // a single static frame
      return;
    }

    // Pause the loop while the hero is scrolled out of view.
    io = new IntersectionObserver(
      function (entries) {
        visible = entries[0].isIntersecting;
        if (visible && !raf) raf = requestAnimationFrame(loop);
      },
      { threshold: 0 }
    );
    io.observe(canvas);

    raf = requestAnimationFrame(loop);
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(start); // instant-navigation observable
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();

// Forze hero — a regular tessellating honeycomb (hexagonal architecture) with a
// gentle pulse expanding radially from behind the headline. A faint always-on
// lattice keeps the structure coherent; the pulse only brightens an outward-
// travelling ring, so it reads as alive but never busy. Pure canvas. Optimised:
// ~30fps, no shadow/mask layers, pauses when off-screen, stills for
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
        return (
            cssVar("--forze-hero-color", "") ||
            cssVar("--md-accent-fg-color", "#ff5722")
        );
    }
    function readBoost() {
        return parseFloat(cssVar("--forze-hero-boost", "1")) || 1;
    }

    function start() {
        var canvas = document.getElementById("forze-hero-canvas");
        if (!canvas) return;

        if (raf) (cancelAnimationFrame(raf), (raf = null));
        if (io) (io.disconnect(), (io = null));
        if (schemeObs) (schemeObs.disconnect(), (schemeObs = null));

        var ctx = canvas.getContext("2d");
        var reduce = window.matchMedia(
            "(prefers-reduced-motion: reduce)",
        ).matches;
        var color = readColor();
        var boost = readBoost();

        var w = 0,
            h = 0,
            hexes = [],
            verts = [],
            last = 0,
            visible = true;

        function seed() {
            var R = Math.max(20, Math.min(w, h) / 15); // hex radius
            var dx = Math.sqrt(3) * R; // pointy-top column spacing
            var dy = 1.5 * R; // row spacing
            verts = [];
            for (var k = 0; k < 6; k++) {
                var ang = (Math.PI / 180) * (60 * k - 90);
                verts.push([Math.cos(ang) * R, Math.sin(ang) * R]);
            }
            // Pulse origin — behind the headline, centre-left. Rings expand from here.
            var ox = w * 0.7;
            var oy = h * 0.52;
            var maxD =
                Math.hypot(Math.max(ox, w - ox), Math.max(oy, h - oy)) || 1;

            hexes = [];
            var row = 0;
            for (var y = -dy; y < h + dy; y += dy, row++) {
                var off = row % 2 ? dx / 2 : 0;
                for (var x = -dx; x < w + dx; x += dx) {
                    // exact tessellation — no jitter, so the lattice reads as one honeycomb
                    var px = x + off;
                    var py = y;
                    // normalized distance from the pulse origin (drives the ring phase)
                    var d = Math.hypot(px - ox, py - oy) / maxD;
                    // gentle vignette so the far edges fade out (never overwhelming)
                    var f = Math.max(0, Math.min(1, 1.15 - d * 1.2));
                    f = f * f * (3 - 2 * f); // smoothstep falloff
                    hexes.push({ x: px, y: py, d: d, f: f });
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

        var RINGS = 20; // spatial frequency: ~one pulse ring crossing the field at a time
        function draw(t) {
            ctx.clearRect(0, 0, w, h);
            ctx.strokeStyle = color;
            ctx.lineWidth = boost > 1.3 ? 1.15 : 1;
            for (var i = 0; i < hexes.length; i++) {
                var hx = hexes[i];
                if (hx.f < 0.02) continue;
                // radial pulse: the bright band sits where (t - d*RINGS) peaks, so it
                // travels outward from the origin as t grows; cubed to sharpen it.
                var s = 0.5 + 0.5 * Math.sin(t - hx.d * RINGS);
                s = s * s * s;
                // faint base keeps the whole honeycomb visible (coherent), + the pulse
                var a = (0.05 + 0.3 * s) * hx.f * boost;
                ctx.globalAlpha = a > 1 ? 1 : a;
                ctx.beginPath();
                ctx.moveTo(hx.x + verts[0][0], hx.y + verts[0][1]);
                for (var k = 1; k < 6; k++)
                    ctx.lineTo(hx.x + verts[k][0], hx.y + verts[k][1]);
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
                draw(now * 0.001); // gentle pulse (~8s per ring)
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
            { threshold: 0 },
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

// Force the dark scheme on the home page (and hide the palette toggle there),
// while preserving the visitor's choice on every other page. Rather than
// re-implement Material's palette logic, we mirror its source of truth — the
// checked palette radio — so the body scheme always matches the selector.
// Also measures the header/tabs height so the home hero fills exactly one
// screen (no scroll). Needed because navigation.instant keeps the body across
// page changes, so per-page scheme must be (re)applied in JS.
(function () {
  function isHome() {
    return !!document.querySelector(".forze-hero");
  }

  function checkedRadio() {
    return document.querySelector(
      'form[data-md-component="palette"] input[name="__palette"]:checked'
    );
  }

  function applyUserPalette(body) {
    var r = checkedRadio();
    if (!r) return; // no toggle yet — leave whatever Material set
    ["scheme", "primary", "accent"].forEach(function (k) {
      var v = r.getAttribute("data-md-color-" + k);
      if (v) body.setAttribute("data-md-color-" + k, v);
    });
  }

  function setTop() {
    var header = document.querySelector(".md-header");
    var tabs = document.querySelector(".md-tabs");
    var top = header ? header.offsetHeight : 0;
    if (tabs && getComputedStyle(tabs).display !== "none") top += tabs.offsetHeight;
    document.body.style.setProperty("--forze-top", top + "px");
  }

  function apply() {
    var body = document.body;
    if (!body) return;
    if (isHome()) {
      body.classList.add("forze-home");
      body.setAttribute("data-md-color-scheme", "slate");
      setTop();
    } else {
      body.classList.remove("forze-home");
      applyUserPalette(body);
    }
  }

  window.addEventListener("resize", function () {
    if (isHome()) setTop();
  });

  if (window.document$ && typeof window.document$.subscribe === "function") {
    // Runs after Material's own palette handler (we load after the bundle), so
    // the checked radio already reflects the visitor's choice. Fires on initial
    // load and on every instant navigation.
    window.document$.subscribe(apply);
  } else {
    if (document.readyState !== "loading") apply();
    document.addEventListener("DOMContentLoaded", apply);
  }
})();

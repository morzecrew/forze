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

  // The visitor's chosen palette. Prefer Material's persisted `__palette` (a
  // synchronous localStorage read) over the checked radio: during an instant
  // navigation the radio may not be re-applied yet when document$ fires, so
  // reading it would bail and leave the body stuck on the home page's forced
  // slate (toggle says "light", page stays dark until a refresh).
  function userColor() {
    try {
      var p = window.__md_get && window.__md_get("__palette");
      if (p && p.color && p.color.scheme) return p.color;
    } catch (e) {}
    var r = checkedRadio();
    if (!r) return null;
    return {
      scheme: r.getAttribute("data-md-color-scheme"),
      primary: r.getAttribute("data-md-color-primary"),
      accent: r.getAttribute("data-md-color-accent"),
    };
  }

  function applyUserPalette(body) {
    // Fall back to the light default: off-home the body is only ever slate
    // because the home page forced it, so an unknown choice means "undo that".
    var c = userColor() || { scheme: "default" };
    ["scheme", "primary", "accent"].forEach(function (k) {
      if (c[k]) body.setAttribute("data-md-color-" + k, c[k]);
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

// Version warning banner -----------------------------------------------------
// Owns the `[data-md-component=outdated]` banner end to end: detects the docs
// version from the URL, decides dev / older-release / latest from mike's
// versions.json, picks the message, adds a dismiss button, and sets visibility.
// Driven by document$ so it is correct on the first load AND every instant
// navigation — the theme's built-in handling is parse-time only (and keyed off
// `base_url`, which `site_url` pins to the site root), so it neither detects the
// version nor stays consistent under navigation.instant.
(function () {
  var versionsPromise = null;

  // Split the path into the deploy root and the version segment, e.g.
  // "/forze/dev/recipes/x/" -> ["/forze/", "dev"].
  function locate() {
    return location.pathname.match(
      /^(.*?\/)(dev|latest|stable|\d+(?:\.\d+)+)(?:\/|$)/
    );
  }

  function loadVersions(root) {
    if (!versionsPromise) {
      versionsPromise = fetch(root + "versions.json", { credentials: "same-origin" })
        .then(function (r) {
          return r.ok ? r.json() : [];
        })
        .catch(function () {
          return [];
        });
    }
    return versionsPromise;
  }

  function applyBanner() {
    var box = document.querySelector('[data-md-component="outdated"]');
    if (!box) return;

    var m = locate();
    if (!m) {
      box.hidden = true;
      return;
    }
    var root = m[1]; // e.g. "/forze/"
    var version = m[2]; // "dev" | "0.4" | "latest" | ...

    loadVersions(root).then(function (versions) {
      var latest = versions.filter(function (v) {
        return (v.aliases || []).indexOf("latest") !== -1;
      })[0];

      // No versions.json (e.g. a plain local build) => treat as latest = hide.
      var isLatest =
        !latest ||
        version === "latest" ||
        version === "stable" ||
        version === latest.version ||
        (latest.aliases || []).indexOf(version) !== -1;
      var isDev = version === "dev";

      // "Go to latest" -> the deploy root, which redirects to the default version.
      box.querySelectorAll("[data-forze-latest]").forEach(function (a) {
        a.setAttribute("href", root);
      });

      // Reveal the message for this channel.
      box.querySelectorAll("[data-forze-channel]").forEach(function (el) {
        el.hidden = (el.getAttribute("data-forze-channel") === "dev") !== isDev;
      });

      // Dismiss button (added once).
      if (!box.querySelector(".forze-banner__close")) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "forze-banner__close";
        btn.setAttribute("aria-label", "Dismiss this notice");
        btn.innerHTML = "&times;";
        btn.addEventListener("click", function () {
          box.hidden = true;
          try {
            sessionStorage.setItem("forze-banner:" + version, "1");
          } catch (e) {}
        });
        box.appendChild(btn);
      }

      var dismissed = false;
      try {
        dismissed = sessionStorage.getItem("forze-banner:" + version) === "1";
      } catch (e) {}

      box.hidden = isLatest || dismissed;
    });
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(applyBanner);
  } else {
    if (document.readyState !== "loading") applyBanner();
    document.addEventListener("DOMContentLoaded", applyBanner);
  }
})();

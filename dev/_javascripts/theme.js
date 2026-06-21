// Force the dark scheme on the home page (and hide the palette toggle there),
// while preserving the visitor's choice on every other page. Rather than
// re-implement Material's palette logic, we mirror its source of truth — the
// checked palette radio — so the body scheme always matches the selector.
// Also measures the header/tabs height so the home hero fills exactly one
// screen (no scroll). Needed because navigation.instant keeps the body across
// page changes, so per-page scheme must be (re)applied in JS.
(function () {
  function isHome() {
    // Matches the current hero (.fz-hero) and the legacy one (.forze-hero).
    return !!document.querySelector(".fz-hero, .forze-hero");
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
    // Re-stamped every run so the once-created dismiss button reads the CURRENT
    // page's version at click time, not the one captured when it was created.
    box.dataset.bannerVersion = version;

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
            sessionStorage.setItem(
              "forze-banner:" + box.dataset.bannerVersion,
              "1"
            );
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

// Version switcher: open on click, not hover ---------------------------------
// mike/Material opens the version list on :hover, which is fiddly. We drive it
// with a click-toggled `.md-version--open` class instead (the stylesheet maps
// that class to the open state and neutralizes the hover trigger). Delegated on
// document so it works for the selector mike injects and survives instant nav;
// clicking the current version toggles it, clicking elsewhere or Escape closes.
(function () {
  document.addEventListener("click", function (e) {
    var onCurrent =
      e.target.closest && e.target.closest(".md-version__current");
    if (onCurrent) e.preventDefault();
    document.querySelectorAll(".md-version").forEach(function (v) {
      if (onCurrent && v.contains(onCurrent)) {
        v.classList.toggle("md-version--open");
      } else {
        v.classList.remove("md-version--open");
      }
    });
  });

  document.addEventListener("keydown", function (e) {
    if (e.key !== "Escape") return;
    document.querySelectorAll(".md-version--open").forEach(function (v) {
      v.classList.remove("md-version--open");
    });
  });
})();

// Version switcher: pin the current version to the top -----------------------
// Move the version you're viewing to the top of the dropdown, then a separator,
// then the rest in their original order. The list is injected asynchronously by
// the theme, so wait for it; the reorder is idempotent (guarded by the
// separator) and re-runs on instant navigation.
(function () {
  function firstText(el) {
    for (var n = el.firstChild; n; n = n.nextSibling) {
      if (n.nodeType === 3 && n.textContent.trim()) return n.textContent.trim();
    }
    return (el.textContent || "").trim();
  }

  function reorder() {
    var version = document.querySelector(".md-version");
    if (!version) return false;
    var list = version.querySelector(".md-version__list");
    var current = version.querySelector(".md-version__current");
    if (!list || !current) return false;
    var items = list.querySelectorAll(".md-version__item");
    if (!items.length) return false; // not populated yet — keep waiting
    if (list.querySelector(".forze-version-sep")) return true; // already done

    // The current version is the one whose title matches the trigger button.
    var curTitle = firstText(current);
    var selected = null;
    for (var i = 0; i < items.length; i++) {
      var link = items[i].querySelector(".md-version__link");
      if (link && firstText(link) === curTitle) {
        selected = items[i];
        break;
      }
    }
    if (!selected) return true; // nothing matched — leave the list as-is

    list.insertBefore(selected, list.firstChild);
    var sep = document.createElement("li");
    sep.className = "forze-version-sep";
    sep.setAttribute("aria-hidden", "true");
    list.insertBefore(sep, selected.nextSibling);
    return true;
  }

  function init() {
    if (reorder()) return;
    var obs = new MutationObserver(function () {
      if (reorder()) obs.disconnect();
    });
    obs.observe(document.body, { childList: true, subtree: true });
    setTimeout(function () {
      obs.disconnect();
    }, 8000); // safety: stop watching even if the selector never appears
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(init);
  } else if (document.readyState !== "loading") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();

// Version switcher: don't 404 when a page is missing in the target version ----
// Each version entry links to the *same* path in that version; if that page
// doesn't exist there, you'd land on a 404. Intercept the click (capture phase,
// ahead of the theme's own handler), HEAD the target, and fall back to that
// version's home when it's missing. Cross-version is a full load anyway.
(function () {
  // The path after the current version segment, e.g. "get-started/changelog/".
  function relPath() {
    var m = location.pathname.match(
      /^.*?\/(?:dev|latest|stable|\d+(?:\.\d+)+)\/(.*)$/,
    );
    return m ? m[1] : "";
  }

  document.addEventListener(
    "click",
    function (e) {
      var link = e.target.closest && e.target.closest(".md-version__link");
      if (!link || !link.href) return;
      // Only hijack a plain left click — let the browser handle modified clicks
      // (Cmd/Ctrl/Shift/Alt, middle button) so open-in-new-tab still works.
      if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey)
        return;
      e.preventDefault();
      e.stopImmediatePropagation();
      document.querySelectorAll(".md-version--open").forEach(function (v) {
        v.classList.remove("md-version--open");
      });

      // The theme links each entry to the target version's *root*. Rebuild the
      // same path under that root so we can keep your place when it exists.
      var home = link.href;
      var rel = relPath();
      var samePath = rel ? home.replace(/\/?$/, "/") + rel : home;
      if (samePath === home) {
        location.href = home;
        return;
      }

      var done = false;
      function go(url) {
        if (done) return;
        done = true;
        location.href = url;
      }
      // Stay on the page only for a confirmed 200; anything else (404, a slow
      // check, an error) goes to the version home, so we never land on a 404.
      var timer = setTimeout(function () {
        go(home);
      }, 4000);
      fetch(samePath, { method: "GET", cache: "no-store" })
        .then(function (r) {
          clearTimeout(timer);
          go(r.status === 200 ? samePath : home);
        })
        .catch(function () {
          clearTimeout(timer);
          go(home);
        });
    },
    true,
  );
})();

// Sidebars: a compact "scroll for more" indicator instead of a scrollbar -------
// Appended to each scroll container; shown only when there is more content
// below, hidden at the end. Re-evaluated on scroll, resize, and instant
// navigation (the TOC changes per page). The scrollbar itself is hidden in CSS.
(function () {
  function setup(wrap) {
    if (wrap.__forzeMore) {
      requestAnimationFrame(wrap.__forzeMore);
      return;
    }

    // A zero-height `position: sticky; bottom: 0` marker placed at the end of the
    // scroll content; the browser's compositor pins it to the visible bottom edge
    // (no JS repositioning -> no jump/flicker). JS only toggles visibility.
    var inner = wrap.querySelector(".md-sidebar__inner") || wrap;
    var ind = document.createElement("div");
    ind.className = "forze-scroll-more";
    ind.setAttribute("aria-hidden", "true");
    var label = document.createElement("span");
    label.className = "forze-scroll-more__label";
    label.textContent = "↓ Scroll for more";
    ind.appendChild(label);
    inner.appendChild(ind);

    var raf = 0;
    function update() {
      raf = 0;
      var more = wrap.scrollHeight - wrap.clientHeight - wrap.scrollTop > 8;
      ind.classList.toggle("is-visible", more);
    }
    function schedule() {
      if (!raf) raf = requestAnimationFrame(update);
    }
    wrap.__forzeMore = update;
    wrap.addEventListener("scroll", schedule, { passive: true });
    window.addEventListener("resize", schedule);
    requestAnimationFrame(update);
  }

  function setupAll() {
    document.querySelectorAll(".md-sidebar__scrollwrap").forEach(setup);
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(setupAll);
  } else if (document.readyState !== "loading") {
    setupAll();
  } else {
    document.addEventListener("DOMContentLoaded", setupAll);
  }
})();

// Image lightbox (diagrams & figures) ----------------------------------------
// The theme bundle constructs a single GLightbox once and, on instant
// navigation, only calls setElements() — which refreshes the gallery array but
// does NOT rebind click listeners to the freshly swapped DOM nodes. So after a
// navigation the first click on a diagram falls through to the raw .svg. We own
// the interaction instead: a capture-phase delegated click handler intercepts
// every `.glightbox` anchor (queried at click time, so always current and never
// per-page-bound), suppresses the bundle's stale element listener with
// stopImmediatePropagation, and opens a fresh lightbox built from the clicked
// element's gallery. The `#only-light` / `#only-dark` fragment is dropped — the
// visible (clickable) anchor already encodes the right theme. If the library
// failed to load, the click falls through to opening the file directly.
(function () {
  var SELECTOR = "a.glightbox";
  var lb = null;

  function galleryFor(anchor) {
    var g = anchor.getAttribute("data-gallery");
    var sel = g ? SELECTOR + '[data-gallery="' + g + '"]' : SELECTOR;
    return Array.prototype.slice.call(document.querySelectorAll(sel));
  }

  document.addEventListener(
    "click",
    function (e) {
      var anchor = e.target.closest && e.target.closest(SELECTOR);
      if (!anchor) return;
      // Leave modified clicks (open-in-new-tab etc.) and the no-library case
      // to their default behaviour.
      if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey)
        return;
      if (typeof GLightbox === "undefined") return;

      e.preventDefault();
      e.stopImmediatePropagation(); // beat the bundle's stale element listener

      var items = galleryFor(anchor);
      var elements = items.map(function (el) {
        return {
          href: (el.getAttribute("href") || "").split("#")[0],
          type: "image",
          title: el.getAttribute("data-title") || "",
        };
      });

      if (lb) lb.destroy();
      lb = GLightbox({
        elements: elements,
        touchNavigation: true,
        loop: false,
        zoomable: true,
        draggable: true,
        openEffect: "zoom",
        closeEffect: "zoom",
      });
      lb.openAt(Math.max(0, items.indexOf(anchor)));
    },
    true, // capture phase — runs before the anchor's own (bundle) listener
  );
})();

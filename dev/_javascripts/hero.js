/* =========================================================================
   Forze — custom homepage interactions
   Drop into:  docs/javascripts/forze-home.js
   Register in mkdocs.yml:
     extra_javascript:
       - javascripts/forze-home.js
   Self-contained, no dependencies. Safe to load on every page — it no-ops
   when the hero markup isn't present.
   ========================================================================= */
(function () {
    "use strict";

    var reduceMotion =
        window.matchMedia &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function init() {
        var root = document.querySelector(".fz");
        if (!root) return; // not the homepage

        buildMarquee(root);
        hexGrid(root);
        adapterSwap(root);
        reveal(root);
        countUp(root);
        cardGlow(root);
    }

    /* ---------------------------------------------------------------------
       0. Integration marquee — real brand logos from theSVG.org, rendered as
          monochrome masks so they pick up the steel→orange chip styling.
          Built in JS (then duplicated) to keep a seamless, edit-once loop.
       --------------------------------------------------------------------- */
    function buildMarquee(root) {
        var track = root.querySelector(".fz-marquee__track[data-logos]");
        if (!track || track.childElementCount) return;

        var CDN = "https://thesvg.org/icons/";
        // Docs live under {site}/integrations/<page>/ — relative to the
        // homepage ("/forze/"), so a bare "integrations/<page>/" href resolves
        // correctly regardless of the deployment base path.
        var DOCS = "integrations/";
        // [label, slug, page] — slug resolves to {CDN}{slug}/default.svg;
        // page resolves to {DOCS}{page}/ (omit to render a non-linked chip).
        var items = [
            ["PostgreSQL", "postgresql", "postgres"],
            ["MongoDB", "mongodb", "mongo"],
            ["Firestore", "firestore", "firestore"],
            ["Neo4j", "neo4j", "neo4j"],
            ["Redis", "redis", "redis"],
            ["Amazon S3", "aws", "s3"],
            ["Cloud Storage", "google-cloud", "gcs"],
            ["Meilisearch", "meilisearch", "meilisearch"],
            ["BigQuery", "google-bigquery", "bigquery"],
            ["ClickHouse", "clickhouse", "clickhouse"],
            ["RabbitMQ", "rabbitmq", "rabbitmq"],
            ["Amazon SQS", "aws", "sqs"],
            ["Temporal", "temporal", "temporal"],
            ["Inngest", "inngest", "inngest"],
            ["FastAPI", "fastapi", "fastapi"],
            ["Socket.IO", "socketdotio", "socketio"],
            ["MCP", "model-context-protocol", "mcp"],
        ];

        function chip(label, slug, page) {
            // Anchor when a docs page exists, plain span otherwise — both keep
            // the .fz-chip styling.
            var el = document.createElement(page ? "a" : "span");
            el.className = "fz-chip";
            if (page) el.href = DOCS + page + "/";
            el.style.setProperty(
                "--logo",
                "url('" + CDN + slug + "/default.svg')",
            );
            var i = document.createElement("i");
            el.appendChild(i);
            el.appendChild(document.createTextNode(label));
            return el;
        }

        // two identical sets => -50% transform loops seamlessly
        for (var pass = 0; pass < 2; pass++) {
            for (var n = 0; n < items.length; n++) {
                track.appendChild(chip(items[n][0], items[n][1], items[n][2]));
            }
        }
    }

    /* ---------------------------------------------------------------------
       1. Animated hexagonal grid on a canvas.
          Cells gently breathe; a few light up in orange and fade out,
          echoing Forze's "hexagonal architecture" identity.
       --------------------------------------------------------------------- */
    function hexGrid(root) {
        var canvas = root.querySelector(".fz-hero__canvas");
        if (!canvas) return;
        var ctx = canvas.getContext("2d");
        var dpr = Math.min(window.devicePixelRatio || 1, 2);

        var R = 30; // hexagon radius
        var cells = []; // {cx, cy, lit, life}
        var W = 0,
            H = 0;
        var sparkClock = 0;

        // Embers drifting up from the lower-left "heat source" (the forge).
        // Reuses this canvas + RAF loop, so there's no extra GPU cost.
        var embers = [];
        var MAX_EMBERS = 40;
        function spawnEmber() {
            embers.push({
                x: W * (Math.random() * 0.3), // hug the left edge
                y: H * (0.82 + Math.random() * 0.22), // start low
                vx: 0.06 + Math.random() * 0.22, // drift right
                vy: -(0.18 + Math.random() * 0.4), // rise
                r: 0.6 + Math.random() * 1.4,
                life: 0,
                ttl: 80 + Math.random() * 160,
            });
        }

        function buildGrid() {
            var rect = canvas.getBoundingClientRect();
            W = rect.width;
            H = rect.height;
            canvas.width = Math.floor(W * dpr);
            canvas.height = Math.floor(H * dpr);
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

            cells = [];
            var hStep = R * 1.5;
            var vStep = R * Math.sqrt(3);
            var col = 0;
            for (var x = -R; x < W + R; x += hStep) {
                var offset = col % 2 ? vStep / 2 : 0;
                for (var y = -R; y < H + R; y += vStep) {
                    cells.push({ cx: x, cy: y + offset, lit: 0, target: 0 });
                }
                col++;
            }
        }

        function hexPath(cx, cy, r) {
            ctx.beginPath();
            for (var i = 0; i < 6; i++) {
                var a = (Math.PI / 180) * (60 * i);
                var px = cx + r * Math.cos(a);
                var py = cy + r * Math.sin(a);
                if (i === 0) ctx.moveTo(px, py);
                else ctx.lineTo(px, py);
            }
            ctx.closePath();
        }

        function frame() {
            ctx.clearRect(0, 0, W, H);

            // occasionally light a random cell
            sparkClock++;
            if (sparkClock > 8 && cells.length) {
                sparkClock = 0;
                var n = 1 + Math.floor(Math.random() * 2);
                for (var k = 0; k < n; k++) {
                    var c = cells[(Math.random() * cells.length) | 0];
                    c.target = 1;
                }
            }

            for (var i = 0; i < cells.length; i++) {
                var cell = cells[i];
                // ease toward target, then decay
                cell.lit += (cell.target - cell.lit) * 0.08;
                if (cell.target === 1 && cell.lit > 0.92) cell.target = 0;

                // Cool steel by default; heats toward red-hot orange as a cell lights.
                var lit = cell.lit;
                var r = Math.round(126 + (255 - 126) * lit);
                var g = Math.round(155 + (110 - 155) * lit);
                var b = Math.round(196 + (40 - 196) * lit);
                var a = 0.06 + lit * 0.82;
                hexPath(cell.cx, cell.cy, R - 1);
                ctx.strokeStyle =
                    "rgba(" + r + "," + g + "," + b + "," + a + ")";
                ctx.lineWidth = 1;
                ctx.stroke();

                if (lit > 0.04) {
                    hexPath(cell.cx, cell.cy, R - 3);
                    ctx.fillStyle =
                        "rgba(" +
                        r +
                        "," +
                        g +
                        "," +
                        b +
                        "," +
                        lit * 0.12 +
                        ")";
                    ctx.fill();
                }
            }

            // embers / sparks rising from the lower-left forge glow
            if (embers.length < MAX_EMBERS && Math.random() < 0.25)
                spawnEmber();
            for (var e = embers.length - 1; e >= 0; e--) {
                var p = embers[e];
                p.life++;
                p.x += p.vx;
                p.y += p.vy;
                p.vy -= 0.0008; // gentle updraft acceleration
                p.vx += (Math.random() - 0.5) * 0.01; // slight flicker drift
                var k = p.life / p.ttl;
                if (k >= 1) {
                    embers.splice(e, 1);
                    continue;
                }
                var fade = Math.sin(Math.min(k, 1) * Math.PI); // fade in then out
                ctx.beginPath();
                ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
                ctx.fillStyle =
                    "rgba(255," +
                    (120 + (((1 - k) * 60) | 0)) +
                    ",50," +
                    fade * 0.7 +
                    ")";
                ctx.shadowColor = "rgba(255,110,40,0.8)";
                ctx.shadowBlur = 6;
                ctx.fill();
                ctx.shadowBlur = 0;
            }

            raf = requestAnimationFrame(frame);
        }

        var raf;
        buildGrid();

        if (reduceMotion) {
            // draw a single static frame
            for (var i = 0; i < cells.length; i++) {
                hexPath(cells[i].cx, cells[i].cy, R - 1);
                ctx.strokeStyle = "rgba(126,155,196,0.08)";
                ctx.lineWidth = 1;
                ctx.stroke();
            }
        } else {
            frame();
        }

        var resizeTimer;
        window.addEventListener("resize", function () {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(buildGrid, 150);
        });

        // pause when the tab is hidden to save battery
        document.addEventListener("visibilitychange", function () {
            if (document.hidden) {
                cancelAnimationFrame(raf);
            } else if (!reduceMotion) {
                raf = requestAnimationFrame(frame);
            }
        });
    }

    /* ---------------------------------------------------------------------
       2. Adapter swap animation — the heart of Forze's pitch.
          Cycles the infrastructure token (Postgres -> Mongo -> ...) while
          the surrounding business logic stays untouched.
       --------------------------------------------------------------------- */
    function adapterSwap(root) {
        var el = root.querySelector(".fz-swap");
        if (!el) return;

        var adapters = [
            "PostgresDepsModule",
            "MongoDepsModule",
            "FirestoreDepsModule",
            "RedisDepsModule",
        ];
        var idx = 0;

        if (reduceMotion) {
            el.textContent = adapters[0];
            return;
        }

        var caret = document.createElement("span");
        caret.className = "fz-caret";

        function setText(t) {
            el.textContent = t;
        }

        function typeIn(word, done) {
            var i = 0;
            setText("");
            el.appendChild(caret);
            (function step() {
                if (i <= word.length) {
                    caret.remove();
                    setText(word.slice(0, i));
                    el.appendChild(caret);
                    i++;
                    setTimeout(step, 45);
                } else {
                    el.classList.add("is-on");
                    setTimeout(function () {
                        caret.remove();
                        done();
                    }, 1400);
                }
            })();
        }

        function deleteOut(done) {
            el.classList.remove("is-on");
            var word = el.textContent;
            var i = word.length;
            el.appendChild(caret);
            (function step() {
                if (i >= 0) {
                    setText(word.slice(0, i));
                    el.appendChild(caret);
                    i--;
                    setTimeout(step, 28);
                } else {
                    done();
                }
            })();
        }

        function loop() {
            typeIn(adapters[idx], function () {
                deleteOut(function () {
                    idx = (idx + 1) % adapters.length;
                    setTimeout(loop, 250);
                });
            });
        }

        // start once the code window scrolls into view
        var started = false;
        var io = new IntersectionObserver(
            function (entries) {
                entries.forEach(function (e) {
                    if (e.isIntersecting && !started) {
                        started = true;
                        loop();
                        io.disconnect();
                    }
                });
            },
            { threshold: 0.35 },
        );
        io.observe(el.closest(".fz-code") || el);
    }

    /* ---------------------------------------------------------------------
       3. Reveal-on-scroll
       --------------------------------------------------------------------- */
    function reveal(root) {
        var items = root.querySelectorAll(".fz-reveal");
        if (!items.length) return;
        if (reduceMotion || !("IntersectionObserver" in window)) {
            items.forEach(function (el) {
                el.classList.add("fz-in");
            });
            return;
        }
        var io = new IntersectionObserver(
            function (entries) {
                entries.forEach(function (e) {
                    if (e.isIntersecting) {
                        e.target.classList.add("fz-in");
                        io.unobserve(e.target);
                    }
                });
            },
            { threshold: 0.15, rootMargin: "0px 0px -8% 0px" },
        );
        items.forEach(function (el) {
            io.observe(el);
        });
    }

    /* ---------------------------------------------------------------------
       4. Count-up stats
       --------------------------------------------------------------------- */
    function countUp(root) {
        var nums = root.querySelectorAll("[data-count]");
        if (!nums.length) return;

        function run(el) {
            var target = parseFloat(el.getAttribute("data-count"));
            var suffix = el.getAttribute("data-suffix") || "";
            var dur = 1300;
            var start = performance.now();
            if (reduceMotion) {
                el.textContent = format(target) + suffix;
                return;
            }
            function tick(now) {
                var p = Math.min((now - start) / dur, 1);
                var eased = 1 - Math.pow(1 - p, 3);
                el.textContent = format(target * eased) + suffix;
                if (p < 1) requestAnimationFrame(tick);
                else el.textContent = format(target) + suffix;
            }
            requestAnimationFrame(tick);
        }
        function format(v) {
            return Number.isInteger(v) ? String(Math.round(v)) : v.toFixed(1);
        }

        var io = new IntersectionObserver(
            function (entries) {
                entries.forEach(function (e) {
                    if (e.isIntersecting) {
                        run(e.target);
                        io.unobserve(e.target);
                    }
                });
            },
            { threshold: 0.6 },
        );
        nums.forEach(function (el) {
            io.observe(el);
        });
    }

    /* ---------------------------------------------------------------------
       5. Cursor-follow glow on feature cards
       --------------------------------------------------------------------- */
    function cardGlow(root) {
        var cards = root.querySelectorAll(".fz-card");
        cards.forEach(function (card) {
            card.addEventListener("pointermove", function (e) {
                var r = card.getBoundingClientRect();
                card.style.setProperty("--mx", e.clientX - r.left + "px");
                card.style.setProperty("--my", e.clientY - r.top + "px");
            });
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }

    // MkDocs Material uses instant navigation (SPA-like). Re-init on page change.
    if (window.document$ && typeof window.document$.subscribe === "function") {
        window.document$.subscribe(function () {
            init();
        });
    }
})();

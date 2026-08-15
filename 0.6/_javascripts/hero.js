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
        var cells = []; // {cx, cy, lit, target, hot}
        var W = 0,
            H = 0;
        var sparkClock = 0;

        // Steel base every cell rests at when unlit (matches the old default).
        var STEEL = [126, 155, 196];

        // Per-cell "hot" colour is sampled from the page's hero gradient
        // (linear-gradient(114deg, #859dc1, #8e7276, --fz-orange, --fz-orange-2)).
        // Resolve each stop to RGB once via a hidden probe so theme/accent vars
        // (and var() nesting) are honoured.
        function resolveColor(expr) {
            var probe = document.createElement("span");
            probe.style.cssText = "display:none;color:" + expr;
            root.appendChild(probe);
            var c = getComputedStyle(probe).color; // "rgb(r, g, b)"
            probe.remove();
            var m = c.match(/\d+(\.\d+)?/g) || [126, 155, 196];
            return [+m[0], +m[1], +m[2]];
        }

        var GRAD = [
            [0.0, resolveColor("#859dc1")],
            [0.49, resolveColor("#8e7276")],
            [0.79, resolveColor("var(--fz-orange)")],
            [1.0, resolveColor("var(--fz-orange-2)")],
        ];

        // Sample the gradient at t in [0,1] -> [r,g,b].
        function sampleGradient(t) {
            t = t < 0 ? 0 : t > 1 ? 1 : t;
            for (var i = 1; i < GRAD.length; i++) {
                if (t <= GRAD[i][0]) {
                    var a = GRAD[i - 1],
                        b = GRAD[i];
                    var f = (t - a[0]) / (b[0] - a[0] || 1);
                    return [
                        a[1][0] + (b[1][0] - a[1][0]) * f,
                        a[1][1] + (b[1][1] - a[1][1]) * f,
                        a[1][2] + (b[1][2] - a[1][2]) * f,
                    ];
                }
            }
            return GRAD[GRAD.length - 1][1].slice();
        }

        // Vertical palette controls:
        //   WARM_FLOOR   - bottom fraction of the grid kept steel (0..1).
        //   WARM_SKEW    - <1 skews the palette toward orange, so orange
        //                  dominates quickly once above the floor (1 = linear).
        //   WARM_SCATTER - per-cell random spread, so it reads as a
        //                  probability distribution rather than a hard line.
        var WARM_FLOOR = 0.2;
        var WARM_SKEW = 0.3;
        var WARM_SCATTER = 0.25;

        // Blink controls — how cells spark and fade:
        //   SPARK_INTERVAL - frames between sparks (lower = blinks more often).
        //   SPARK_MIN/MAX  - inclusive range of cells lit per spark.
        //   BLINK_SPEED    - ease rate toward lit/unlit (higher = snappier).
        //   BLINK_PEAK     - brightness at which a cell starts fading back out.
        var SPARK_INTERVAL = 40;
        var SPARK_MIN = 1;
        var SPARK_MAX = 2;
        var BLINK_SPEED = 0.03;
        var BLINK_PEAK = 0.82;

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
                    var cy = y + offset;
                    // 0 at the bottom edge, 1 at the top of the grid block.
                    var fromBottom = H ? (H - cy) / H : 0;
                    // Stay steel through the bottom WARM_FLOOR band, then skew
                    // hard toward orange so it dominates above the floor.
                    var bias = Math.max(
                        0,
                        Math.min(
                            1,
                            (fromBottom - WARM_FLOOR) / (1 - WARM_FLOOR),
                        ),
                    );
                    bias = Math.pow(bias, WARM_SKEW);
                    // Per-cell scatter -> a probability distribution, not a line:
                    // low cells mostly steel, higher cells mostly orange.
                    var t = Math.max(
                        0,
                        Math.min(
                            1,
                            bias + (Math.random() * 2 - 1) * WARM_SCATTER,
                        ),
                    );
                    cells.push({
                        cx: x,
                        cy: cy,
                        lit: 0,
                        target: 0,
                        hot: sampleGradient(t),
                    });
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
            if (sparkClock > SPARK_INTERVAL && cells.length) {
                sparkClock = 0;
                var n =
                    SPARK_MIN +
                    Math.floor(Math.random() * (SPARK_MAX - SPARK_MIN + 1));
                for (var k = 0; k < n; k++) {
                    var c = cells[(Math.random() * cells.length) | 0];
                    c.target = 1;
                }
            }

            for (var i = 0; i < cells.length; i++) {
                var cell = cells[i];
                // ease toward target, then decay
                cell.lit += (cell.target - cell.lit) * BLINK_SPEED;
                if (cell.target === 1 && cell.lit > BLINK_PEAK) cell.target = 0;

                // Cool steel by default; heats toward this cell's gradient
                // colour as it lights — lower cells barely warm (their hot
                // colour is near steel), higher cells flare orange.
                var lit = cell.lit;
                var hot = cell.hot;
                var r = Math.round(STEEL[0] + (hot[0] - STEEL[0]) * lit);
                var g = Math.round(STEEL[1] + (hot[1] - STEEL[1]) * lit);
                var b = Math.round(STEEL[2] + (hot[2] - STEEL[2]) * lit);
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

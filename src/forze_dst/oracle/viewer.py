"""A self-contained HTML time-travel viewer for a counterexample — a scrubber over the timeline.

Builds on work already done: :func:`~forze_dst.oracle.report.build_timeline` flattens a run into
virtual-time-ordered steps and :meth:`~forze_dst.oracle.report.TimelineEntry.to_dict` makes each
JSON-able. This embeds that stream in a single HTML file (no external assets, no build step) with a
small vanilla-JS scrubber: step through the run by virtual time, see each step's structured detail,
the way a debugger walks a trace. Open it in any browser; attach it to a CI artifact.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from forze_dst.oracle.report import build_timeline

if TYPE_CHECKING:
    from forze_dst.oracle.replay import ViolationReport

# ----------------------- #

_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>DST time-travel</title>
<style>
  :root{--bg:#0d1117;--panel:#161b22;--fg:#c9d1d9;--muted:#8b949e;--line:#30363d}
  *{box-sizing:border-box}
  body{margin:0;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;background:var(--bg);color:var(--fg)}
  header{padding:14px 18px;border-bottom:1px solid var(--line)}
  header h1{margin:0 0 4px;font-size:15px}
  .meta{color:var(--muted);font-size:12px}
  .viol{margin-top:8px;color:#ff7b72}
  .wrap{display:flex;height:calc(100vh - 150px)}
  .list{width:56%;overflow:auto;border-right:1px solid var(--line)}
  .step{padding:5px 18px;cursor:pointer;border-left:3px solid transparent;white-space:pre;overflow:hidden;text-overflow:ellipsis}
  .step:hover{background:#1c2128}
  .step.cur{background:#1f2937;border-left-color:#58a6ff}
  .t{color:var(--muted)}
  .kind{display:inline-block;width:11ch}
  .k-op{color:#58a6ff}.k-call{color:#79c0ff}.k-fault{color:#ff7b72}.k-latency{color:#d29922}.k-partition{color:#bc8cff}.k-fact{color:#3fb950}
  .detail{width:44%;overflow:auto;padding:14px 18px}
  .detail pre{background:var(--panel);padding:12px;border-radius:6px;border:1px solid var(--line);overflow:auto;white-space:pre-wrap;word-break:break-word}
  .bar{display:flex;align-items:center;gap:12px;padding:12px 18px;border-top:1px solid var(--line)}
  .bar input[type=range]{flex:1}
  button{background:var(--panel);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:5px 11px;cursor:pointer}
  button:hover{border-color:#58a6ff}
</style>
</head>
<body>
<header>
  <h1 id="title"></h1>
  <div class="meta" id="sub"></div>
  <div class="viol" id="viol"></div>
</header>
<div class="wrap">
  <div class="list" id="list"></div>
  <div class="detail"><div class="meta" id="dlabel"></div><pre id="ddetail"></pre></div>
</div>
<div class="bar">
  <button id="prev">&#9664; prev</button>
  <input type="range" id="slider" min="0" value="0">
  <button id="next">next &#9654;</button>
  <span class="meta" id="pos"></span>
</div>
<script type="application/json" id="dst-data">__DST_DATA__</script>
<script>
(function(){
  var D=JSON.parse(document.getElementById('dst-data').textContent), T=D.timeline||[];
  var glyph={op:'\\u25B8',call:'\\u21B3',fault:'\\u26A1',latency:'\\u23F1',partition:'\\u2702',fact:'\\u2022'};
  function esc(s){return String(s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c]})}
  document.getElementById('title').textContent='DST time-travel \\u2014 seed '+D.seed+(D.schedule_seed!=null?(' \\u00B7 schedule '+D.schedule_seed):'');
  document.getElementById('sub').textContent=T.length+' steps'+(D.workload?(' \\u00B7 '+D.workload.length+'-op minimized workload'):'')+(D.fingerprint?(' \\u00B7 registry '+String(D.fingerprint).slice(0,12)+'\\u2026'):'');
  document.getElementById('viol').textContent=(D.violations||[]).map(function(v){return '\\u2717 '+v.invariant+': '+v.message}).join('    ');
  var list=document.getElementById('list'), cur=-1, slider=document.getElementById('slider');
  T.forEach(function(e,i){
    var d=document.createElement('div'); d.className='step'; d.title=e.label;
    d.innerHTML='<span class="t">@'+(+e.at).toFixed(6)+'</span>  <span class="kind k-'+e.kind+'">'+(glyph[e.kind]||'\\u00B7')+' '+e.kind+'</span>  '+esc(e.label);
    d.onclick=function(){select(i)}; list.appendChild(d);
  });
  slider.max=Math.max(0,T.length-1);
  function select(i){
    if(!T.length)return; i=Math.max(0,Math.min(T.length-1,i)); cur=i; slider.value=i;
    for(var j=0;j<list.children.length;j++){list.children[j].classList.toggle('cur',j===i)}
    var e=T[i];
    document.getElementById('dlabel').textContent='@'+(+e.at).toFixed(6)+'  '+e.kind+'  \\u00B7  step '+(i+1)+'/'+T.length;
    document.getElementById('ddetail').textContent=JSON.stringify(e.detail,null,2);
    document.getElementById('pos').textContent=(i+1)+' / '+T.length;
    if(list.children[i])list.children[i].scrollIntoView({block:'nearest'});
  }
  document.getElementById('prev').onclick=function(){select(cur-1)};
  document.getElementById('next').onclick=function(){select(cur+1)};
  slider.oninput=function(){select(+slider.value)};
  document.addEventListener('keydown',function(ev){if(ev.key==='ArrowLeft')select(cur-1);if(ev.key==='ArrowRight')select(cur+1)});
  if(T.length){select(0)}else{document.getElementById('ddetail').textContent='(no steps recorded)'}
})();
</script>
</body>
</html>
"""

# ....................... #


def _report_data(report: ViolationReport) -> dict[str, Any]:
    """The JSON-able view a viewer steps through — metadata, violations, workload, timeline."""

    return {
        "seed": report.seed,
        "schedule_seed": report.schedule_seed,
        "fingerprint": report.registry_fingerprint,
        "violations": [{"invariant": v.invariant, "message": v.message} for v in report.violations],
        "workload": [
            (
                list(item)  # pyright: ignore[reportUnknownArgumentType]
                if isinstance(item, tuple)
                else [item, None]
            )
            for item in report.workload
        ],
        "timeline": [entry.to_dict() for entry in build_timeline(report.history)],
    }


# ....................... #


def render_html(report: ViolationReport) -> str:
    """Render *report* as a single self-contained HTML time-travel viewer (string).

    Embeds the virtual-time timeline + counterexample metadata; no external assets. Write it to a
    ``.html`` file and open it in a browser to scrub the run step by step.
    """

    blob = json.dumps(_report_data(report), default=str, ensure_ascii=False)
    # Keep the JSON from breaking out of the <script type="application/json"> block.
    blob = blob.replace("</", "<\\/")

    return _TEMPLATE.replace("__DST_DATA__", blob)

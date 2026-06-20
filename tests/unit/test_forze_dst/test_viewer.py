"""The HTML time-travel viewer + the copy-paste repro snippet in ``report.format()``."""

from __future__ import annotations

from forze_dst.invariants import Violation
from forze_dst.oracle import ViolationReport, render_html
from forze_dst.oracle.recorder import Event, History

# ----------------------- #


def _report(*, seed: int = 7, extra_events: tuple[Event, ...] = ()) -> ViolationReport:
    history = History(
        seed=seed,
        events=(
            Event(
                seq=0,
                kind="operation",
                at=0.0,
                fields={"op": "pay", "outcome": "ok", "invoked_at": 0.0, "call_id": 0},
            ),
            Event(
                seq=1,
                kind="trace",
                at=0.1,
                fields={
                    "trace_domain": "document",
                    "surface": "document_command",
                    "op": "create",
                    "phase": "command",
                },
            ),
            Event(seq=2, kind="balance", at=0.2, fields={"final": 2, "expected": 1}),
            *extra_events,
        ),
    )
    return ViolationReport(
        seed=seed,
        schedule_seed=3,
        violations=(Violation(invariant="no_double_charge", message="charged twice"),),
        workload=(("pay", None),),
        history=history,
        registry_fingerprint="abc123def456ghi",
    )


class TestRenderHtml:
    def test_is_self_contained_html_with_the_data_embedded(self) -> None:
        html = render_html(_report())
        assert html.startswith("<!doctype html>")
        assert 'id="dst-data"' in html  # the embedded timeline JSON
        assert '"seed": 7' in html
        assert "charged twice" in html  # the violation surfaces in the header
        # No external assets — everything inline.
        assert "http://" not in html and "https://" not in html

    def test_json_cannot_break_out_of_the_script_tag(self) -> None:
        # A label carrying a literal </script> must be neutralised, not close the data block early.
        evil = Event(seq=3, kind="a</script>b", at=0.3, fields={"x": 1})
        html = render_html(_report(extra_events=(evil,)))
        # Only the template's two real </script> tags — the injected one was escaped to <\/script>.
        assert html.count("</script>") == 2
        assert "<\\/script>" in html

    def test_to_html_writes_a_file(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        out = tmp_path / "viewer.html"
        returned = _report().to_html(out)
        assert out.exists()
        assert out.read_text(encoding="utf-8") == returned
        assert returned.startswith("<!doctype html>")


class TestReproSnippet:
    def test_format_includes_a_copy_pasteable_repro(self) -> None:
        text = _report(seed=42).format()
        assert "reproduce:" in text
        assert "SimulationConfig.reproduce(42)" in text
        assert "assert_no_violation" in text

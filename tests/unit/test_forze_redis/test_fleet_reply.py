"""Parsing the fleet scripts' one-string reply."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from forze.base.exceptions import CoreException
from forze_redis.adapters.fleet import parse_fleet_reply

# ----------------------- #


class TestParseFleetReply:
    def test_reads_the_server_time_and_each_member(self) -> None:
        view = parse_fleet_reply(
            "1700000000123\n"
            'b\t1700000010000\tstarting|1700000000000|{"load":2}\n'
            "a\t1700000010000\tserving|1699999990000|{}"
        )

        assert view.at == datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)
        assert [(m.id, m.state, dict(m.report)) for m in view.members] == [
            ("a", "serving", {}),
            ("b", "starting", {"load": 2}),
        ]
        assert view.members[0].since == datetime(2023, 11, 14, 22, 13, 10, tzinfo=UTC)

    def test_an_empty_fleet_is_the_time_alone(self) -> None:
        assert parse_fleet_reply("1700000000000").members == ()

    def test_a_report_keeps_separators_json_leaves_raw(self) -> None:
        view = parse_fleet_reply('1\na\t2\tserving|1|{"note":"x y|z\\tw"}')

        assert view.members[0].report == {"note": "x y|z\tw"}

    @pytest.mark.parametrize(
        "line",
        [
            "a\t2\tpaused|1|{}",
            "a\t2\tserving|1",
            "a\t2\tserving|soon|{}",
            "a\t2\tserving|1|[1]",
            "a\t2\tserving|1|{not json",
            "a only",
        ],
    )
    def test_a_line_it_cannot_read_is_skipped(self, line: str) -> None:
        view = parse_fleet_reply(f"5\n{line}\nb\t9\tserving|1|{{}}")

        assert [m.id for m in view.members] == ["b"]

    def test_a_reply_without_the_server_time_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            parse_fleet_reply("[b'1', b'2']")

        assert caught.value.code == "fleet.reply_malformed"

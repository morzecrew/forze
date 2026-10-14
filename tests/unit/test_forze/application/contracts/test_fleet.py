"""Fleet membership: the policies over a view, and the checks every backend shares."""

from __future__ import annotations

import json
import math
import os
import subprocess  # nosec B404 — fixed argv, used to run the rank in fresh interpreters
import sys
from datetime import UTC, datetime, timedelta

import pytest

from forze.application.contracts.fleet import (
    FleetSpec,
    FleetView,
    Member,
    MemberState,
    decode_report,
    encode_report,
    prepare_beat,
    validate_member_id,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_AT = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


def _member(
    member_id: str,
    state: MemberState = "serving",
    *,
    age: timedelta = timedelta(minutes=5),
    **report: int | float | str,
) -> Member:
    return Member(id=member_id, state=state, since=_AT - age, report=report)


def _view(*members: Member) -> FleetView:
    return FleetView(members=members, at=_AT)


# ....................... #


class TestFleetView:
    def test_members_are_sorted_by_id(self) -> None:
        view = _view(_member("c"), _member("a"), _member("b"))

        assert [m.id for m in view.members] == ["a", "b", "c"]

    def test_report_is_a_read_only_copy(self) -> None:
        source = {"load": 1}
        member = Member(id="a", state="serving", since=_AT, report=source)
        source["load"] = 2

        assert member.report == {"load": 1}

        with pytest.raises(TypeError):
            member.report["load"] = 3  # type: ignore[index]


class TestSettled:
    def test_an_empty_view_is_not_settled(self) -> None:
        assert not _view().settled(timedelta(0))

    @pytest.mark.parametrize("state", ["starting", "draining"])
    def test_any_member_not_serving_unsettles_the_fleet(self, state: MemberState) -> None:
        assert not _view(_member("a"), _member("b", state)).settled(timedelta(0))

    def test_a_member_serving_for_less_than_for_unsettles_the_fleet(self) -> None:
        view = _view(_member("a"), _member("b", age=timedelta(seconds=29)))

        assert not view.settled(timedelta(seconds=30))
        assert view.settled(timedelta(seconds=29))


class TestShare:
    @pytest.mark.parametrize("members", [1, 2, 3, 7])
    @pytest.mark.parametrize("total", [0, 1, 5, 6, 100, 101])
    def test_shares_sum_to_total_and_differ_by_at_most_one(self, members: int, total: int) -> None:
        ids = [f"m{i}" for i in range(members)]
        view = _view(*(_member(i) for i in ids))
        shares = [view.share(total, me) for me in ids]

        assert sum(shares) == total
        assert max(shares) - min(shares) <= 1

    def test_the_remainder_goes_to_the_first_ids(self) -> None:
        view = _view(_member("b"), _member("a"), _member("c"))

        assert [view.share(5, me) for me in ("a", "b", "c")] == [2, 2, 1]

    def test_a_member_not_serving_gets_nothing_and_is_not_counted(self) -> None:
        view = _view(_member("a"), _member("b", "draining"), _member("c", "starting"))

        assert view.share(4, "a") == 4
        assert view.share(4, "b") == 0
        assert view.share(4, "c") == 0
        assert view.share(4, "absent") == 0


class TestRank:
    def test_a_one_member_fleet_ranks_itself_first_for_every_key(self) -> None:
        view = _view(_member("only"))

        assert all(view.rank(f"tenant-{i}") == ("only",) for i in range(50))

    def test_states_decide_the_candidates(self) -> None:
        view = _view(_member("a"), _member("b", "starting"), _member("c", "draining"))

        assert set(view.rank("k")) == {"a"}
        assert set(view.rank("k", states=("serving", "starting"))) == {"a", "b"}
        assert view.rank("k", states=()) == ()

    def test_removing_a_member_moves_only_its_keys(self) -> None:
        ids = [f"replica-{i}" for i in range(5)]
        keys = [f"tenant-{i}" for i in range(500)]
        before = _view(*(_member(i) for i in ids))
        after = _view(*(_member(i) for i in ids if i != "replica-2"))

        moved = [k for k in keys if before.rank(k)[0] != after.rank(k)[0]]

        assert moved
        assert all(before.rank(k)[0] == "replica-2" for k in moved)
        # Its keys go to their next successor, not anywhere.
        assert all(after.rank(k)[0] == before.rank(k)[1] for k in moved)

    def test_rank_matches_golden_vectors(self) -> None:
        # Computed apart from the code, with hashlib, from the formula: blake2b (8-byte
        # digest, big-endian) over ``key + "\0" + member_id``, highest first.
        view = _view(*(_member(f"gw-{s}") for s in "abcde"))

        assert view.rank("t0") == ("gw-a", "gw-e", "gw-b", "gw-d", "gw-c")
        assert view.rank("t1") == ("gw-c", "gw-d", "gw-a", "gw-e", "gw-b")

    def test_the_weight_matches_a_golden_value(self) -> None:
        from forze.application.contracts.fleet.value_objects import _weight

        assert _weight("t0", "gw-a") == 12989977397389005085

    def test_rank_is_the_same_in_every_process(self) -> None:
        script = (
            "import json\n"
            "from datetime import UTC, datetime\n"
            "from forze.application.contracts.fleet import FleetView, Member\n"
            "at = datetime(2026, 1, 1, tzinfo=UTC)\n"
            "view = FleetView(members=[Member(id=f'r{i}', state='serving', since=at,"
            " report={}) for i in range(6)], at=at)\n"
            "print(json.dumps([view.rank(f't{k}') for k in range(40)]))\n"
        )
        view = _view(*(_member(f"r{i}") for i in range(6)))
        expected = [list(view.rank(f"t{k}")) for k in range(40)]

        for seed in ("0", "1", "4242", "random"):
            result = subprocess.run(  # nosec B603 — this interpreter, a fixed script
                [sys.executable, "-c", script],
                env={**os.environ, "PYTHONHASHSEED": seed},
                capture_output=True,
                text=True,
                check=True,
                timeout=60,
            )

            assert json.loads(result.stdout) == expected, seed


class TestExcess:
    _SETTLE = timedelta(minutes=1)

    def _fleet(self, *loads: tuple[str, float], **extra: int | float | str) -> FleetView:
        return _view(*(_member(i, load=load, **extra) for i, load in loads))

    def test_excess_over_the_mean_is_rounded_up(self) -> None:
        view = self._fleet(("a", 10), ("b", 10), ("c", 32))  # mean 17.33…

        assert view.excess("c", load="load", settle=self._SETTLE) == 15
        assert view.excess("a", load="load", settle=self._SETTLE) == 0

    def test_nothing_while_the_fleet_is_unsettled(self) -> None:
        view = _view(_member("a", load=10), _member("b", "starting", load=0), _member("c", load=40))

        assert view.excess("c", load="load", settle=self._SETTLE) == 0
        assert view.excess("c", load="load", settle=timedelta(hours=1)) == 0

    def test_nothing_within_the_tolerance(self) -> None:
        view = self._fleet(("a", 100), ("b", 114))  # mean 107, 114 <= 107 * 1.15

        assert view.excess("b", load="load", settle=self._SETTLE) == 0
        assert view.excess("b", load="load", tolerance=0.01, settle=self._SETTLE) == 7

    def test_capped_by_the_movable_report_rounded_down(self) -> None:
        view = _view(
            _member("a", load=0, movable=0),
            _member("b", load=100, movable=3.9),
        )

        result = view.excess("b", load="load", movable="movable", settle=self._SETTLE)

        assert result == 3
        assert type(result) is int

    def test_a_non_numeric_movable_counts_as_absent(self) -> None:
        view = _view(_member("a", load=0), _member("b", load=100, movable="lots"))

        assert view.excess("b", load="load", movable="movable", settle=self._SETTLE) == 50
        assert view.excess("b", load="load", movable="missing", settle=self._SETTLE) == 50

    def test_below_smallest_is_nothing(self) -> None:
        view = self._fleet(("a", 0), ("b", 4))  # mean 2, excess 2

        assert view.excess("b", load="load", settle=self._SETTLE, smallest=2) == 2
        assert view.excess("b", load="load", settle=self._SETTLE, smallest=3) == 0

    def test_the_mean_is_over_members_reporting_a_number(self) -> None:
        view = _view(
            _member("a", load=10),
            _member("b", load="busy"),
            _member("c"),
            _member("d", load=30),
        )

        assert view.excess("d", load="load", settle=self._SETTLE) == 10

    def test_a_bool_load_is_not_a_number(self) -> None:
        # Built by hand: backends refuse a bool before it is ever stored.
        view = _view(_member("a", load=0), _member("b", load=True), _member("c", load=1))

        assert view.excess("b", load="load", settle=self._SETTLE) == 0
        # True would have counted as 1, making the mean 2/3 and the excess 1.
        assert view.excess("c", load="load", settle=self._SETTLE, tolerance=0) == 1

    def test_an_integer_past_any_float_is_not_a_load(self) -> None:
        view = _view(_member("a", load=10**400), _member("b", load=1), _member("c", load=5))

        assert view.excess("a", load="load", settle=self._SETTLE) == 0
        assert view.excess("c", load="load", settle=self._SETTLE) == 2

    def test_loads_near_the_float_limit_do_not_overflow_the_mean(self) -> None:
        view = self._fleet(("a", 1e308), ("b", 1e308), ("c", 1.7e308))
        mean = 1e308 / 3 + 1e308 / 3 + 1.7e308 / 3

        assert view.excess("c", load="load", settle=self._SETTLE) == math.ceil(1.7e308 - mean)

    def test_never_negative_whatever_smallest_and_movable_say(self) -> None:
        view = _view(_member("a", load=0), _member("b", load=10, movable=-3))

        assert (
            view.excess("b", load="load", movable="movable", settle=self._SETTLE, smallest=-5)
            == 0
        )

    def test_a_member_without_the_load_gets_nothing(self) -> None:
        view = _view(_member("a", load=0), _member("b"))

        assert view.excess("b", load="load", settle=self._SETTLE) == 0
        assert view.excess("absent", load="load", settle=self._SETTLE) == 0


# ....................... #


class TestFleetSpec:
    def test_defaults(self) -> None:
        spec = FleetSpec(name="gateways")

        assert spec.ttl == timedelta(seconds=10)
        assert spec.report_max_bytes == 1024

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"ttl": timedelta(0)},
            {"ttl": timedelta(microseconds=999)},
            {"report_max_bytes": 1},
        ],
    )
    def test_refuses_an_unusable_spec(self, kwargs: dict[str, object]) -> None:
        with pytest.raises(CoreException, match="Fleet"):
            FleetSpec(name="gateways", **kwargs)  # type: ignore[arg-type]


class TestMemberId:
    @pytest.mark.parametrize("member_id", ["host:1:ab", "pod-0", "é" * 128])
    def test_accepts(self, member_id: str) -> None:
        validate_member_id(member_id)

    @pytest.mark.parametrize(
        "member_id",
        ["", "a\tb", "a\nb", "a\rb", "{a}", "a}", "x" * 257, "é" * 129, "a\ud800"],
    )
    def test_refuses(self, member_id: str) -> None:
        with pytest.raises(CoreException) as caught:
            validate_member_id(member_id)

        assert caught.value.code == "fleet.member_id_invalid"


class TestReportCodec:
    def test_numbers_round_trip_exactly(self) -> None:
        report = {
            "int": 7,
            "big": 2**70,
            "negative": -3,
            "float": 0.1,
            "whole_float": 2.0,
            "tiny": 5e-324,
            "huge": 1.7976931348623157e308,
            "text": "a\tb\nc|d{e}ё",
        }

        decoded = decode_report(encode_report(report, max_bytes=1024))

        assert decoded == report
        assert {k: type(v) for k, v in decoded.items()} == {k: type(v) for k, v in report.items()}

    def test_negative_zero_keeps_its_sign(self) -> None:
        decoded = decode_report(encode_report({"z": -0.0}, max_bytes=64))

        assert math.copysign(1.0, decoded["z"]) == -1.0  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "report",
        [
            {"flag": True},
            {"flag": False},
            {"nan": math.nan},
            {"inf": math.inf},
            {"ninf": -math.inf},
            {"nested": {"a": 1}},
            {"list": [1]},
            {"none": None},
            {1: 1},
            {"lone": "\ud800"},
        ],
    )
    def test_refuses(self, report: dict[object, object]) -> None:
        with pytest.raises(CoreException) as caught:
            encode_report(report, max_bytes=1024)  # type: ignore[arg-type]

        assert caught.value.code == "fleet.report_invalid"

    def test_refuses_an_integer_past_the_interpreters_digit_limit(self) -> None:
        with pytest.raises(CoreException) as caught:
            encode_report({"n": 10**5000}, max_bytes=10**6)

        assert caught.value.code == "fleet.report_invalid"

    @pytest.mark.parametrize(
        "raw", ['{"x":[1]}', '{"x":NaN}', '{"x":Infinity}', '{"x":true}', "[1]", '"x"']
    )
    def test_decode_refuses_what_encode_would_not_write(self, raw: str) -> None:
        with pytest.raises(ValueError):
            decode_report(raw)

    def test_refuses_a_report_over_the_cap_in_bytes(self) -> None:
        report = {"k": "é" * 10}  # {"k":"…"} is 8 bytes plus 20 for the value

        assert encode_report(report, max_bytes=28)

        with pytest.raises(CoreException, match="29 bytes"):
            encode_report({"k": "é" * 10 + "x"}, max_bytes=28)


class TestPrepareBeat:
    _SPEC = FleetSpec(name="f", ttl=timedelta(seconds=3), report_max_bytes=64)

    def test_returns_the_encoded_report_and_the_ttl_in_milliseconds(self) -> None:
        assert prepare_beat(self._SPEC, "a", "serving", {"n": 1}, None) == ('{"n":1}', 3000)
        assert prepare_beat(self._SPEC, "a", "draining", {}, timedelta(milliseconds=1.9)) == (
            "{}",
            1,
        )

    @pytest.mark.parametrize(
        ("args", "code"),
        [
            (("a\t", "serving", {}, None), "fleet.member_id_invalid"),
            (("a", "paused", {}, None), "fleet.state_invalid"),
            (("a", "serving", {}, timedelta(microseconds=999)), "fleet.ttl_invalid"),
            (("a", "serving", {}, timedelta(seconds=-1)), "fleet.ttl_invalid"),
            (("a", "serving", {"x": "y" * 64}, None), "fleet.report_invalid"),
        ],
    )
    def test_refuses(self, args: tuple[object, ...], code: str) -> None:
        with pytest.raises(CoreException) as caught:
            prepare_beat(self._SPEC, *args)  # type: ignore[arg-type]

        assert caught.value.code == code

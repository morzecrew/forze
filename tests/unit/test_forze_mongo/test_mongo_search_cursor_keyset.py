"""Unit tests for Mongo search cursor seek conditions."""

from forze_mongo.adapters.search._cursor_seek import build_keyset_seek_match


def test_keyset_seek_after_desc_rank() -> None:
    match = build_keyset_seek_match(
        [("_mongo_rank", "desc"), ("id", "asc")],
        [0.9, "abc"],
        after=True,
    )

    assert "$or" in match
    branches = match["$or"]
    assert branches[0] == {"_mongo_rank": {"$lt": 0.9}}

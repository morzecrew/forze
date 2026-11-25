"""Identifiers and comments in SQL that a consumer applies with migration privileges.

The schema commands print SQL from two sources: forze's declarations, and names read back from
a live catalog. The consumer runs that file under a role that can create and drop tables, so a
catalog name that ended a comment or a statement would be an injection (RFC 0080 row 16).
Nothing read from the catalog reaches the output except through :func:`quote_ident` or
:func:`comment`.
"""

import re
import unicodedata
from typing import Final

# ----------------------- #

_BARE: Final = re.compile(r"[a-z_][a-z0-9_]*")
"""A name Postgres's ``quote_ident`` leaves unquoted, unless it is a keyword below."""

_KEYWORD_LIST: Final = """
    all analyse analyze and any array as asc asymmetric authorization between bigint binary bit
    boolean both case cast char character check coalesce collate collation column concurrently
    constraint create cross current_catalog current_date current_role current_schema
    current_time current_timestamp current_user dec decimal default deferrable desc distinct do
    else end except exists extract false fetch float for foreign freeze from full grant greatest
    group grouping having ilike in initially inner inout int integer intersect interval into is
    isnull join json json_array json_arrayagg json_exists json_object json_objectagg json_query
    json_scalar json_serialize json_table json_value lateral leading least left like limit
    localtime localtimestamp merge_action national natural nchar none normalize not notnull null
    nullif numeric offset on only or order out outer overlaps overlay placing position precision
    primary real references returning right row select session_user setof similar smallint some
    substring symmetric system_user table tablesample then time timestamp to trailing treat trim
    true union unique user using values varchar variadic verbose when where window with
    xmlattributes xmlconcat xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi xmlroot
    xmlserialize xmltable
"""
"""Every keyword ``quote_ident`` quotes: ``pg_get_keywords()`` rows whose ``catcode`` is not
``U``, on Postgres 18. Postgres 16 has a subset of these and 17 the same set, so a name is
quoted wherever any supported server would quote it. An integration test compares this list
with the server's own ``quote_ident()``."""

_KEYWORDS: Final = frozenset(_KEYWORD_LIST.split())

# ....................... #


def quote_ident(name: str) -> str:
    """*name* as Postgres's ``quote_ident()`` writes it.

    Bare when it is lower-case letters, digits and underscores, starts with a letter or an
    underscore, and is not a reserved word. Otherwise double-quoted with each inner quote
    doubled. A quoted identifier may hold a newline or a semicolon and still be one token, so
    quoting is enough to keep any catalog name inside the statement it names.
    """

    if _BARE.fullmatch(name) and name not in _KEYWORDS:
        return name

    return '"' + name.replace('"', '""') + '"'


# ....................... #


def unquote_ident(key: str) -> str:
    """The name *key* spells when it is one identifier, quoted or bare: ``"order"`` is
    ``order``. Any other key, an expression or a key with a direction, comes back as it is."""

    if len(key) >= 2 and key.startswith('"') and key.endswith('"'):
        return key[1:-1].replace('""', '"')

    return key


def qualified(schema: str, name: str) -> str:
    """``schema.name``, each part quoted on its own."""

    return f"{quote_ident(schema)}.{quote_ident(name)}"


# ....................... #


def one_line(text: str) -> str:
    """*text* with every control character and line or paragraph separator turned into a
    space.

    Postgres ends a ``--`` comment at CR or LF. Some migration tools split on other line
    breaks too, so every character that could end a line is replaced, not only those two.
    """

    return "".join(
        " " if unicodedata.category(char) in {"Cc", "Zl", "Zp"} else char for char in text
    )


# ....................... #


def comment(text: str) -> str:
    """*text* as one ``--`` comment line, whatever it contains."""

    return f"-- {one_line(text)}".rstrip()

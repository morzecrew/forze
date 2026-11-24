"""Split SQL text into statements the way a SQL lexer does.

A ``;`` ends a statement only outside string literals, quoted identifiers and ``--``
comments, so a quoted name holding ``;`` and a newline stays inside its statement. The schema
tests use this to check that printed SQL is exactly the statements meant.
"""

import re

# ----------------------- #


def split_sql(text: str) -> list[str]:
    """The statements of *text*, without their ``;``, comments left out."""

    statements: list[str] = []
    current: list[str] = []
    position = 0

    while position < len(text):
        char = text[position]

        if char in "'\"":
            end = position + 1

            while True:
                end = text.index(char, end)

                if text.startswith(char * 2, end):
                    end += 2
                    continue

                break

            current.append(text[position : end + 1])
            position = end + 1
            continue

        if text.startswith("--", position):
            newline = re.search(r"[\r\n]", text[position:])
            position = len(text) if newline is None else position + newline.start()
            continue

        if char == ";":
            statements.append("".join(current).strip())
            current = []

        else:
            current.append(char)

        position += 1

    assert not "".join(current).strip(), "text after the last statement"

    return statements

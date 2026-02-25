def normalize_pg_type(base: str) -> str:
    b = base.strip().lower()

    # timestamptz
    if b == "timestamp with time zone":
        return "timestamptz"

    if b == "timestamp without time zone":
        return "timestamp"

    # varchar
    if b.startswith("character varying"):
        return "varchar"

    if b == "character":
        return "char"

    # numeric / float
    if b == "double precision":
        return "float8"

    if b == "real":
        return "float4"

    # ints
    if b == "smallint":
        return "int2"

    if b == "integer":
        return "int4"

    if b == "bigint":
        return "int8"

    # boolean
    if b == "boolean":
        return "bool"

    return b

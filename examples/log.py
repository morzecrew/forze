from forze.base.logging import configure, getLogger

# ----------------------- #

configure(
    level="TRACE",
    colorize=True,
)
log = getLogger("example")


# plain
log.trace("This is a trace message")
log.debug("This is a debug message")
log.info("This is an info message")
log.warning("This is a warning message")
log.error("This is an error message")
log.critical("This is a critical message")

# error with traceback

try:
    raise ValueError("This is an example error")
except ValueError:
    log.exception("This is an exception message")

# critical with traceback
try:
    raise ValueError("This is an example error")
except ValueError:
    log.critical_exception("This is a critical exception message")

# Nested
log.info("Depth 0")

with log.section():
    log.info("Depth 1")

    with log.section():
        log.info("Depth 2")

# Inline plain extra

log.info("This is a message with inline extra", a=1, b=2, c=3)

# Long plain extra

log.debug(
    "This is a message with a long plain extra",
    a=1,
    b=2,
    c=3,
    d=4,
    e=5,
    f=6,
    g=7,
    h=8,
    i=9,
    j=10,
)

# Nested extra

log.info(
    "This is a message with a nested extra",
    nested={
        "a": 1,
        "b": 2,
        "c": 3,
        "d": {"qowroqwrojqworjqowjroqwjrqwr": 124124124124124124124},
    },
)

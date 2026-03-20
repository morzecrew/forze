from forze.base.logging import Logger, configure_logging, install_excepthook

# ----------------------- #

configure_logging(
    render_mode="console",
    level="info",
    logger_names=["forze.example", "forze.uncaught"],
)
install_excepthook()

# ....................... #

logger = Logger("forze.example")

# plain
logger.trace("This is a trace message")
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")
logger.info(
    "This is very long info message with a lot of extra information. This is very long info message with a lot of extra information"
)

# error with traceback

try:
    raise ValueError("This is an example error")

except ValueError:
    logger.exception("This is an exception message")

# critical with traceback
try:
    raise ValueError("This is an example error")

except ValueError:
    logger.critical_exception("This is a critical exception message")

raise ValueError("This is an uncaught error")

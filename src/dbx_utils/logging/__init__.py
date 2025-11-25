"""
A logger based on the standard Python logging module, adjusted for databricks.
"""

import logging

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def getLogger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """Return a pre-configured logger.

    The logger writes to standard output using the ``DEFAULT_LOG_FORMAT``.
    A handler is added only once per logger name to avoid duplicate log entries
    when ``get_logger`` is called multiple times.

    Args:
        name: Optional logger name. If omitted, the module name is used.
        level: Logging level to apply to the logger and its handler. DEBGUG = 10, INFO = 20, WARNING = 30

    Returns:
        A :class:`logging.Logger` instance ready for use.
    """

    logger_name = name or __name__
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Streamhandler makes it safe for parallel tasks and prevents duplicate messages.
    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        logger.addHandler(handler)

    return logger

__all__ = ["getLogger", "DEFAULT_LOG_FORMAT"]
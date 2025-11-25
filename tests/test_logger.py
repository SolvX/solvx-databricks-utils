from dbx_utils.logging import DEFAULT_LOG_FORMAT, getLogger


def test_get_logger_writes_to_stdout(capsys):
    logger = getLogger("sample_logger")
    logger.info("hello from logger")

    captured = capsys.readouterr().out
    assert "sample_logger" in captured
    assert "hello from logger" in captured


def test_get_logger_sets_default_format():
    logger = getlogger("format_logger", level=logging.DEBUG)
    handler = next(
        h for h in logger.handlers if isinstance(h, logging.StreamHandler)
    )
    assert handler.formatter._fmt == DEFAULT_LOG_FORMAT


def test_get_logger_is_idempotent():
    logger = getLogger("idempotent")
    initial_handlers = list(logger.handlers)

    logger_again = getLogger("idempotent")
    assert logger_again is logger
    assert logger_again.handlers == initial_handlers
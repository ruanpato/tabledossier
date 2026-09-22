from tabledossier.errors import error_record, sanitize_message


class _ClassicError(Exception):
    def getErrorClass(self):
        return "TABLE_OR_VIEW_NOT_FOUND"


class _ConnectError(Exception):
    """Like PySpark 3.5 Spark Connect exceptions: no condition attribute, prefixed message."""

    def getErrorClass(self):
        return None


def test_condition_from_exception_attribute():
    record = error_record(_ClassicError("[TABLE_OR_VIEW_NOT_FOUND] missing"), "resolve")
    assert record["condition"] == "TABLE_OR_VIEW_NOT_FOUND"
    assert record["error_class"] == "_ClassicError"


def test_condition_from_message_prefix_when_the_client_does_not_expose_it():
    exc = _ConnectError(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `a`.`b` cannot be found.\nJVM stacktrace:"
    )
    record = error_record(exc, "resolve")
    assert record["condition"] == "TABLE_OR_VIEW_NOT_FOUND"
    assert record["message"].startswith("[TABLE_OR_VIEW_NOT_FOUND] The table or view `a`.`b`")
    assert "JVM" not in record["message"]
    sub = error_record(ValueError("[INVALID_SQL_SYNTAX.MULTI_PART_NAME] x"), "resolve")
    assert sub["condition"] == "INVALID_SQL_SYNTAX.MULTI_PART_NAME"
    assert error_record(ValueError("[not a condition] x"), "resolve")["condition"] is None
    assert error_record(ValueError("plain failure"), "resolve")["condition"] is None


def test_messages_are_sanitized():
    text = "[CAST_INVALID_INPUT] The value 'SECRET-123' of type \"STRING\" at s3://bucket/key"
    cleaned = sanitize_message(text)
    assert "SECRET" not in cleaned and "s3://" not in cleaned
    assert error_record(ValueError(text), "aggregate")["condition"] == "CAST_INVALID_INPUT"

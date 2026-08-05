import json
import logging
from typing import Any

import pytest

from app.core.logging import JsonFormatter


def _record(**kwargs: Any) -> logging.LogRecord:
    record = logging.LogRecord(
        name="app.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=kwargs.pop("msg", "hello"),
        args=kwargs.pop("args", ()),
        exc_info=kwargs.pop("exc_info", None),
    )
    for key, value in kwargs.items():
        setattr(record, key, value)
    return record


def test_emits_valid_json_with_base_fields() -> None:
    entry = json.loads(JsonFormatter().format(_record()))
    assert entry["message"] == "hello"
    assert entry["level"] == "INFO"
    assert entry["logger"] == "app.test"
    assert entry["ts"].endswith("+00:00")


def test_ctx_is_flattened_into_entry() -> None:
    entry = json.loads(JsonFormatter().format(_record(ctx={"payment_id": "abc", "attempt": 2})))
    assert entry["payment_id"] == "abc"
    assert entry["attempt"] == 2


def test_non_dict_ctx_is_ignored() -> None:
    entry = json.loads(JsonFormatter().format(_record(ctx="not-a-dict")))
    assert "ctx" not in entry


def test_exception_is_rendered() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = _record(exc_info=sys.exc_info())
    entry = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in entry["exc_info"]


def test_unserializable_value_does_not_break_logging() -> None:
    entry = json.loads(JsonFormatter().format(_record(ctx={"obj": object()})))
    assert "object at" in entry["obj"]


def test_message_formatting_is_applied() -> None:
    entry = json.loads(JsonFormatter().format(_record(msg="value=%s", args=("x",))))
    assert entry["message"] == "value=x"


@pytest.mark.parametrize("text", ['quote " inside', "русский текст", "{'json': 'like'}"])
def test_special_characters_do_not_break_structure(text: str) -> None:
    entry = json.loads(JsonFormatter().format(_record(ctx={"key": text})))
    assert entry["key"] == text

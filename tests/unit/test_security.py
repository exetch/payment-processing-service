import pytest

from app.core.security import api_key_is_valid

SECRET = "correct-horse-battery-staple"


def test_accepts_exact_key() -> None:
    assert api_key_is_valid(SECRET, SECRET) is True


@pytest.mark.parametrize(
    "provided",
    [
        "",
        "wrong",
        SECRET.upper(),
        SECRET[:-1],
        SECRET + "x",
        f" {SECRET}",
        "correct-horse-battery-stapl",
    ],
)
def test_rejects_anything_else(provided: str) -> None:
    assert api_key_is_valid(provided, SECRET) is False


def test_handles_non_ascii_without_raising() -> None:
    """compare_digest падает на не-ASCII str, поэтому сравниваем байты."""
    assert api_key_is_valid("ключ", SECRET) is False
    assert api_key_is_valid("ключ", "ключ") is True

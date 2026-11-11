"""request_digest must not let extra fields override the canonical keys."""
import pytest

from joons_agent_guard import request_digest


def test_body_sha256_is_reserved():
    with pytest.raises(ValueError):
        request_digest("POST", "/pay", b"amount=1", body_sha256="0" * 64)


def test_extra_fields_must_be_str():
    with pytest.raises(TypeError):
        request_digest("POST", "/pay", b"x", tenant=1)


def test_extra_fields_change_the_digest():
    assert request_digest("POST", "/pay", b"x", tenant="a") != request_digest("POST", "/pay", b"x", tenant="b")

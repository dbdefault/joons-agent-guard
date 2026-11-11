"""Pytest suite for ReplayGuard and request_digest.

Covers the at-most-once inbound protocol (claim/complete/response/apply_pending),
the outbound reservation protocol, identifier validation, digest stability, and
persistence across reopening the same SQLite file.
"""
import sqlite3
from types import MappingProxyType

import pytest

from joons_agent_guard import (
    IntentMismatch,
    InvalidIdentifier,
    ReplayGuard,
    request_digest,
)

BAD_IDS = ["", None, 123, b"principal", 4.5]


# ---------------------------------------------------------------------------
# helpers and fixtures
# ---------------------------------------------------------------------------

def digest(method="POST", path="/things", body=b"{}", **fields):
    """Short alias so call sites stay readable."""
    return request_digest(method, path, body, **fields)


def make_effect(code=200, response=None, note="done"):
    """Build a SQLite-only effect that appends ``note`` to its own table."""
    def effect(conn):
        conn.execute("CREATE TABLE IF NOT EXISTS effect_log (note TEXT NOT NULL)")
        conn.execute("INSERT INTO effect_log(note) VALUES (?)", (note,))
        return code, response
    return effect


def table_names(path):
    """Table names in the guard's database file, read from a fresh connection."""
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return {row[0] for row in rows}
    finally:
        conn.close()


def effect_notes(path):
    """Rows of the effect's own table, or [] when the table does not exist."""
    conn = sqlite3.connect(path)
    try:
        return [row[0] for row in conn.execute("SELECT note FROM effect_log ORDER BY rowid")]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def counting_effect(calls, code=200, response=None):
    """Effect that records each invocation; asserts it never runs unexpectedly."""
    def effect(conn):
        calls.append(1)
        return code, response
    return effect


@pytest.fixture()
def guard():
    """In-memory guard for single-connection behaviour tests."""
    g = ReplayGuard(":memory:")
    yield g
    g.close()


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "guard.db"


@pytest.fixture()
def file_guard(db_path):
    """File-backed guard; needed when a second connection inspects the file."""
    g = ReplayGuard(db_path)
    yield g
    g.close()


# ---------------------------------------------------------------------------
# request_digest
# ---------------------------------------------------------------------------

class TestRequestDigest:
    def test_is_stable_across_calls(self):
        first = request_digest("POST", "/things", b"payload", actor="p1")
        assert first == request_digest("POST", "/things", b"payload", actor="p1")

    def test_is_sixty_four_hex_chars(self):
        d = request_digest("POST", "/things", b"payload")
        assert len(d) == 64
        assert d == d.lower()
        int(d, 16)  # must parse as hex

    def test_sensitive_to_method(self):
        base = request_digest("POST", "/things", b"body")
        assert base != request_digest("PUT", "/things", b"body")

    def test_sensitive_to_path(self):
        base = request_digest("POST", "/things", b"body")
        assert base != request_digest("POST", "/things/1", b"body")

    def test_sensitive_to_body(self):
        base = request_digest("POST", "/things", b"body")
        assert base != request_digest("POST", "/things", b"body2")
        assert base != request_digest("POST", "/things", b"")

    def test_sensitive_to_every_extra_field(self):
        base = request_digest("POST", "/things", b"body", actor="p1", purpose="test")
        assert base != request_digest("POST", "/things", b"body", actor="p2", purpose="test")
        assert base != request_digest("POST", "/things", b"body", actor="p1", purpose="other")
        assert base != request_digest("POST", "/things", b"body", actor="p1")
        assert base != request_digest(
            "POST", "/things", b"body", actor="p1", purpose="test", extra="e")

    def test_field_order_does_not_matter(self):
        assert request_digest("POST", "/t", b"b", a="1", b="2") == \
            request_digest("POST", "/t", b"b", b="2", a="1")

    def test_bytearray_body_matches_bytes(self):
        assert request_digest("POST", "/t", bytearray(b"b")) == request_digest("POST", "/t", b"b")

    def test_empty_bytes_body_is_accepted(self):
        assert request_digest("POST", "/t", b"")

    @pytest.mark.parametrize("bad_body", ["text", "", None, 5, 1.5, memoryview(b"b"), ["b"], {"b": 1}])
    def test_rejects_non_bytes_body(self, bad_body):
        with pytest.raises(TypeError):
            request_digest("POST", "/t", bad_body)


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

class TestClaim:
    def test_first_claim_is_new(self, guard):
        assert guard.claim("p1", "req-1", digest()) == "new"

    def test_reclaim_same_digest_is_pending(self, guard):
        d = digest()
        assert guard.claim("p1", "req-1", d) == "new"
        assert guard.claim("p1", "req-1", d) == "pending"
        assert guard.claim("p1", "req-1", d) == "pending"  # idempotent

    def test_claim_after_complete_is_completed(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        assert guard.complete("p1", "req-1", d, 200, {"ok": 1})
        assert guard.claim("p1", "req-1", d) == "completed"

    def test_conflict_while_pending(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        assert guard.claim("p1", "req-1", d1) == "new"
        assert guard.claim("p1", "req-1", d2) == "conflict"
        # the original claim is untouched and still completable
        assert guard.claim("p1", "req-1", d1) == "pending"
        assert guard.complete("p1", "req-1", d1, 200, {"ok": 1})
        assert guard.claim("p1", "req-1", d2) == "conflict"

    def test_conflict_after_complete(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        guard.claim("p1", "req-1", d1)
        guard.complete("p1", "req-1", d1, 200, {"ok": 1})
        assert guard.claim("p1", "req-1", d2) == "conflict"
        assert guard.claim("p1", "req-1", d1) == "completed"

    def test_conflicting_digest_sees_no_response(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        guard.claim("p1", "req-1", d1)
        guard.complete("p1", "req-1", d1, 200, {"ok": 1})
        assert guard.response("p1", "req-1", d2) is None
        assert guard.response("p1", "req-1", d1) == (200, {"ok": 1})

    def test_same_request_id_different_principals_are_independent(self, guard):
        d = digest()
        assert guard.claim("p1", "req-1", d) == "new"
        assert guard.claim("p2", "req-1", d) == "new"
        assert guard.claim("p1", "req-1", d) == "pending"
        assert guard.claim("p2", "req-1", d) == "pending"


# ---------------------------------------------------------------------------
# complete
# ---------------------------------------------------------------------------

class TestComplete:
    def test_complete_works_exactly_once(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        assert guard.complete("p1", "req-1", d, 200, {"ok": 1}) is True
        assert guard.complete("p1", "req-1", d, 500, {"ok": 0}) is False
        assert guard.response("p1", "req-1", d) == (200, {"ok": 1})  # first response wins

    def test_complete_requires_matching_digest(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        guard.claim("p1", "req-1", d1)
        assert guard.complete("p1", "req-1", d2, 200, {"wrong": True}) is False
        assert guard.claim("p1", "req-1", d1) == "pending"  # stayed pending
        assert guard.response("p1", "req-1", d2) is None
        assert guard.complete("p1", "req-1", d1, 200, {"right": True}) is True

    def test_complete_unknown_request_returns_false(self, guard):
        assert guard.complete("p1", "never-claimed", digest(), 200, {}) is False

    def test_complete_after_completed_with_other_digest_returns_false(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        guard.claim("p1", "req-1", d1)
        guard.complete("p1", "req-1", d1, 200, {"ok": 1})
        assert guard.complete("p1", "req-1", d2, 200, {"other": True}) is False
        assert guard.response("p1", "req-1", d1) == (200, {"ok": 1})


# ---------------------------------------------------------------------------
# response
# ---------------------------------------------------------------------------

class TestResponse:
    @pytest.mark.parametrize("code,payload", [
        (200, {"ok": True, "n": 3}),
        (201, [1, 2, 3]),
        (200, "plain text"),
        (200, "유니코드 응답"),  # ensure_ascii escaping must round-trip
        (204, None),
        (200, {"nested": {"a": [1, {"b": None}]}}),
        (400, 7),
        (200, 2.5),
    ])
    def test_roundtrip_after_complete(self, guard, code, payload):
        d = digest()
        guard.claim("p1", "req-1", d)
        assert guard.complete("p1", "req-1", d, code, payload)
        got = guard.response("p1", "req-1", d)
        assert isinstance(got, tuple) and len(got) == 2
        assert got == (code, payload)
        assert isinstance(got[0], int)

    def test_none_before_claim(self, guard):
        assert guard.response("p1", "req-1", digest()) is None

    def test_none_while_pending(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        assert guard.response("p1", "req-1", d) is None


# ---------------------------------------------------------------------------
# apply_pending
# ---------------------------------------------------------------------------

class TestApplyPending:
    def test_stores_effect_and_response_atomically(self, file_guard, db_path):
        d = digest()
        assert file_guard.claim("p1", "req-1", d) == "new"
        result = file_guard.apply_pending(
            "p1", "req-1", d, make_effect(code=200, response={"ok": True}))
        assert result == (200, {"ok": True})
        assert file_guard.claim("p1", "req-1", d) == "completed"
        assert file_guard.response("p1", "req-1", d) == (200, {"ok": True})
        # the effect's own writes committed together with the stored response
        assert effect_notes(db_path) == ["done"]

    def test_returns_none_when_never_claimed(self, guard):
        calls = []
        assert guard.apply_pending("p1", "req-1", digest(), counting_effect(calls)) is None
        assert calls == []

    def test_returns_none_when_already_completed(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        assert guard.complete("p1", "req-1", d, 200, {"first": True})
        calls = []
        assert guard.apply_pending("p1", "req-1", d, counting_effect(calls)) is None
        assert calls == []
        assert guard.response("p1", "req-1", d) == (200, {"first": True})  # kept

    def test_returns_none_on_digest_mismatch(self, guard):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        guard.claim("p1", "req-1", d1)
        calls = []
        assert guard.apply_pending("p1", "req-1", d2, counting_effect(calls)) is None
        assert calls == []
        assert guard.claim("p1", "req-1", d1) == "pending"

    def test_effect_exception_keeps_pending_and_rolls_back_writes(self, file_guard, db_path):
        d = digest()
        assert file_guard.claim("p1", "req-1", d) == "new"

        def bad_effect(conn):
            conn.execute("CREATE TABLE IF NOT EXISTS effect_log (note TEXT NOT NULL)")
            conn.execute("INSERT INTO effect_log(note) VALUES ('partial')")
            raise RuntimeError("effect failed")

        with pytest.raises(RuntimeError, match="effect failed"):
            file_guard.apply_pending("p1", "req-1", d, bad_effect)

        # the request is still pending, so a reviewed retry is possible
        assert file_guard.claim("p1", "req-1", d) == "pending"
        # the effect's own writes, including the created table, are gone
        assert "effect_log" not in table_names(db_path)
        assert effect_notes(db_path) == []

        # a retry with a good effect then completes exactly once
        result = file_guard.apply_pending(
            "p1", "req-1", d, make_effect(code=200, response={"ok": True}))
        assert result == (200, {"ok": True})
        assert effect_notes(db_path) == ["done"]
        assert file_guard.response("p1", "req-1", d) == (200, {"ok": True})

    def test_effect_runs_at_most_once(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        calls = []

        def effect(conn):
            calls.append(1)
            return 200, {"n": len(calls)}

        assert guard.apply_pending("p1", "req-1", d, effect) == (200, {"n": 1})
        assert guard.apply_pending("p1", "req-1", d, effect) is None
        assert len(calls) == 1

    def test_rejects_bad_identifiers_without_running_effect(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        calls = []
        for bad in BAD_IDS:
            with pytest.raises(InvalidIdentifier):
                guard.apply_pending(bad, "req-1", d, counting_effect(calls))
            with pytest.raises(InvalidIdentifier):
                guard.apply_pending("p1", bad, d, counting_effect(calls))
            with pytest.raises(InvalidIdentifier):
                guard.apply_pending("p1", "req-1", bad, counting_effect(calls))
        assert calls == []
        assert guard.claim("p1", "req-1", d) == "pending"  # state untouched


# ---------------------------------------------------------------------------
# reserve_outbound
# ---------------------------------------------------------------------------

class TestReserveOutbound:
    def test_context_contains_fixed_and_generated_keys(self, guard):
        ctx = guard.reserve_outbound("intent-1", {"endpoint": "/api", "body_sha256": "abc123"})
        assert ctx["endpoint"] == "/api"
        assert ctx["body_sha256"] == "abc123"
        assert isinstance(ctx["request_id"], str) and ctx["request_id"]
        assert isinstance(ctx["ts"], str) and ctx["ts"]

    def test_retry_returns_identical_context(self, guard):
        first = guard.reserve_outbound("intent-1", {"a": "1"})
        assert guard.reserve_outbound("intent-1", {"a": "1"}) == first
        # mapping type and key order must not matter either
        assert guard.reserve_outbound("intent-1", MappingProxyType({"a": "1"})) == first

    def test_different_intents_are_independent(self, guard):
        a = guard.reserve_outbound("intent-a", {"a": "1"})
        b = guard.reserve_outbound("intent-b", {"a": "1"})
        assert a["request_id"] != b["request_id"]

    def test_mismatch_on_changed_value(self, guard):
        guard.reserve_outbound("intent-1", {"a": "1"})
        with pytest.raises(IntentMismatch):
            guard.reserve_outbound("intent-1", {"a": "2"})

    def test_mismatch_on_added_key(self, guard):
        guard.reserve_outbound("intent-1", {"a": "1"})
        with pytest.raises(IntentMismatch):
            guard.reserve_outbound("intent-1", {"a": "1", "b": "2"})

    def test_mismatch_on_removed_key(self, guard):
        guard.reserve_outbound("intent-1", {"a": "1", "b": "2"})
        with pytest.raises(IntentMismatch):
            guard.reserve_outbound("intent-1", {"a": "1"})

    def test_mismatch_keeps_original_reservation(self, guard):
        original = guard.reserve_outbound("intent-1", {"a": "1"})
        with pytest.raises(IntentMismatch):
            guard.reserve_outbound("intent-1", {"a": "2"})
        assert guard.reserve_outbound("intent-1", {"a": "1"}) == original

    @pytest.mark.parametrize("bad_fixed", [
        {"request_id": "x"}, {"ts": "123"}, {"a": "1", "request_id": "x"},
    ])
    def test_rejects_request_id_and_ts_keys(self, guard, bad_fixed):
        with pytest.raises(ValueError):
            guard.reserve_outbound("intent-1", bad_fixed)

    @pytest.mark.parametrize("bad_fixed", [
        {"n": 1}, {"v": None}, {"v": 2.5}, {"v": b"bytes"}, {"v": ["x"]}, {"v": {"k": "v"}},
        {1: "one"}, {("a",): "v"}, "not-a-mapping", ["not", "a", "mapping"], None, 42,
    ])
    def test_rejects_non_str_values_and_non_mappings(self, guard, bad_fixed):
        with pytest.raises(TypeError):
            guard.reserve_outbound("intent-1", bad_fixed)

    def test_empty_fixed_mapping_is_allowed(self, guard):
        first = guard.reserve_outbound("intent-1", {})
        assert set(first) == {"request_id", "ts"}
        assert guard.reserve_outbound("intent-1", {}) == first

    @pytest.mark.parametrize("bad_intent", ["", None, 123, b"intent"])
    def test_rejects_invalid_intent_ids(self, guard, bad_intent):
        with pytest.raises(InvalidIdentifier):
            guard.reserve_outbound(bad_intent, {"a": "1"})


# ---------------------------------------------------------------------------
# persistence across reopening the same DB file
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_completed_state_and_response_survive_reopen(self, db_path):
        d = digest()
        g1 = ReplayGuard(db_path)
        assert g1.claim("p1", "req-1", d) == "new"
        assert g1.complete("p1", "req-1", d, 201, {"saved": True})
        g1.close()

        g2 = ReplayGuard(db_path)
        try:
            assert g2.claim("p1", "req-1", d) == "completed"
            assert g2.response("p1", "req-1", d) == (201, {"saved": True})
        finally:
            g2.close()

    def test_pending_state_survives_reopen_and_is_never_rerun(self, db_path):
        d = digest(body=b"pending")
        g1 = ReplayGuard(db_path)
        assert g1.claim("p1", "req-1", d) == "new"
        g1.close()

        g2 = ReplayGuard(db_path)
        try:
            assert g2.claim("p1", "req-1", d) == "pending"
            assert g2.response("p1", "req-1", d) is None
            assert g2.apply_pending(
                "p1", "req-1", d, make_effect(code=200, response={"ok": 1})) == (200, {"ok": 1})
        finally:
            g2.close()

    def test_conflict_survives_reopen(self, db_path):
        d1, d2 = digest(body=b"one"), digest(body=b"two")
        g1 = ReplayGuard(db_path)
        assert g1.claim("p1", "req-1", d1) == "new"
        g1.close()

        g2 = ReplayGuard(db_path)
        try:
            assert g2.claim("p1", "req-1", d2) == "conflict"
        finally:
            g2.close()

    def test_outbound_reservation_survives_reopen(self, db_path):
        g1 = ReplayGuard(db_path)
        first = g1.reserve_outbound("intent-1", {"a": "1"})
        g1.close()

        g2 = ReplayGuard(db_path)
        try:
            assert g2.reserve_outbound("intent-1", {"a": "1"}) == first
        finally:
            g2.close()

    def test_separate_files_are_independent(self, tmp_path):
        d = digest()
        with ReplayGuard(tmp_path / "a.db") as ga, ReplayGuard(tmp_path / "b.db") as gb:
            assert ga.claim("p1", "req-1", d) == "new"
            assert gb.claim("p1", "req-1", d) == "new"

    def test_context_manager_closes_and_persists(self, db_path):
        d = digest()
        with ReplayGuard(db_path) as g:
            assert g.claim("p1", "req-1", d) == "new"
        g2 = ReplayGuard(db_path)
        try:
            assert g2.claim("p1", "req-1", d) == "pending"
        finally:
            g2.close()


# ---------------------------------------------------------------------------
# identifier validation on inbound methods
# ---------------------------------------------------------------------------

class TestIdentifierValidation:
    def test_claim_rejects_bad_identifiers(self, guard):
        for bad in BAD_IDS:
            with pytest.raises(InvalidIdentifier):
                guard.claim(bad, "req-1", "digest-1")
            with pytest.raises(InvalidIdentifier):
                guard.claim("p1", bad, "digest-1")
            with pytest.raises(InvalidIdentifier):
                guard.claim("p1", "req-1", bad)
        # nothing was claimed by the failed calls
        assert guard.claim("p1", "req-1", "digest-1") == "new"

    def test_complete_rejects_bad_identifiers(self, guard):
        d = digest()
        guard.claim("p1", "req-1", d)
        for bad in BAD_IDS:
            with pytest.raises(InvalidIdentifier):
                guard.complete(bad, "req-1", d, 200, {})
            with pytest.raises(InvalidIdentifier):
                guard.complete("p1", bad, d, 200, {})
            with pytest.raises(InvalidIdentifier):
                guard.complete("p1", "req-1", bad, 200, {})
        assert guard.claim("p1", "req-1", d) == "pending"  # state untouched

    def test_response_rejects_bad_identifiers(self, guard):
        for bad in BAD_IDS:
            with pytest.raises(InvalidIdentifier):
                guard.response(bad, "req-1", "digest-1")
            with pytest.raises(InvalidIdentifier):
                guard.response("p1", bad, "digest-1")
            with pytest.raises(InvalidIdentifier):
                guard.response("p1", "req-1", bad)

    def test_invalid_identifier_is_value_error(self, guard):
        # documented contract: InvalidIdentifier doubles as a ValueError
        with pytest.raises(ValueError):
            guard.claim("", "req-1", "digest-1")

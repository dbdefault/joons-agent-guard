"""Durable replay guard: run each (principal, request_id) at most once.

Guarantees and limits
---------------------
* ``claim`` returns ``new`` exactly once per (principal, request_id).
* A claimed request stays ``pending`` until ``complete``/``apply_pending``.
  Pending means *unknown*: it is never re-run or unlocked by age.
* ``apply_pending`` runs a SQLite-only effect and stores the response in the
  same transaction, so that path is exactly-once.
* Any other effect (files, network, subprocess) is NOT atomic with the
  stored response. A crash between the effect and ``complete`` leaves the
  request pending; your integration must reconcile it.
* The guard does not authenticate anybody. ``principal`` must come from a
  trusted transport (mTLS identity, verified signature, ...), never from a
  client-supplied header.
"""
import hashlib
import json
import time
import uuid
from typing import Any, Callable, Mapping, Optional, Tuple

from . import _db
from .errors import CorruptStorage, IntentMismatch, require_ids

NEW, PENDING, COMPLETED, CONFLICT = "new", "pending", "completed", "conflict"

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS requests (principal TEXT NOT NULL, request_id TEXT NOT NULL, "
    "digest TEXT NOT NULL, state TEXT NOT NULL, code INTEGER, response TEXT, claimed REAL, "
    "PRIMARY KEY(principal, request_id))",
    "CREATE TABLE IF NOT EXISTS outbound_requests (intent_id TEXT PRIMARY KEY, context TEXT NOT NULL)",
)


def _require_code(code):
    if not isinstance(code, int) or isinstance(code, bool):
        raise TypeError("code must be an int")


#: Default cap for one stored response (JSON-encoded), in bytes.
DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024


def request_digest(method: str, path: str, body: bytes, **fields: str) -> str:
    """SHA-256 over a canonical JSON of the request; use it as ``digest``."""
    if not isinstance(body, (bytes, bytearray)):
        raise TypeError("body must be bytes")
    if "body_sha256" in fields:
        raise ValueError("'body_sha256' is reserved and cannot be passed as an extra field")
    if not all(isinstance(v, str) for v in fields.values()):
        raise TypeError("extra fields must be str")
    canon = json.dumps(
        {"method": method, "path": path, "body_sha256": hashlib.sha256(body).hexdigest(), **fields},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    )
    return hashlib.sha256(canon.encode("ascii")).hexdigest()


class ReplayGuard:
    def __init__(self, path, *, timeout: float = 15.0,
                 max_response_bytes: Optional[int] = DEFAULT_MAX_RESPONSE_BYTES):
        """``max_response_bytes`` caps each stored JSON response (None = no cap)."""
        self._max_response_bytes = max_response_bytes
        self._db = _db.connect(path, timeout)
        for stmt in _SCHEMA:
            self._db.execute(stmt)

    # -- inbound ----------------------------------------------------------
    def claim(self, principal: str, request_id: str, digest: str) -> str:
        """Return ``new`` (run it), ``pending``, ``completed`` (return stored response) or ``conflict``."""
        require_ids(principal=principal, request_id=request_id, digest=digest)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "INSERT INTO requests(principal, request_id, digest, state, claimed) "
                "VALUES (?,?,?,'pending',?) ON CONFLICT(principal, request_id) DO NOTHING",
                (principal, request_id, digest, time.time()),
            )
            if cur.rowcount:
                return NEW
            stored = self._db.execute(
                "SELECT digest, state FROM requests WHERE principal=? AND request_id=?",
                (principal, request_id),
            ).fetchone()
        return stored[1] if stored[0] == digest else CONFLICT

    def complete(self, principal: str, request_id: str, digest: str, code: int, response: Any) -> bool:
        """Store the response of a pending request. False if not pending or digest differs.

        ``response`` is stored as JSON: tuples come back as lists and non-str
        dict keys as strings, so use JSON-stable types.
        """
        require_ids(principal=principal, request_id=request_id, digest=digest)
        _require_code(code)
        encoded = self._encode(response)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "UPDATE requests SET state='completed', code=?, response=? "
                "WHERE principal=? AND request_id=? AND digest=? AND state='pending'",
                (code, encoded, principal, request_id, digest),
            )
        return cur.rowcount == 1

    def response(self, principal: str, request_id: str, digest: str) -> Optional[Tuple[int, Any]]:
        require_ids(principal=principal, request_id=request_id, digest=digest)
        row = _db.query_one(
            self._db,
            "SELECT code, response FROM requests WHERE principal=? AND request_id=? AND digest=? "
            "AND state='completed'", (principal, request_id, digest),
        )
        if not row:
            return None
        try:
            return row[0], json.loads(row[1])
        except (TypeError, ValueError):
            raise CorruptStorage(
                f"stored response for request {request_id!r} of principal {principal!r} cannot be decoded"
            ) from None

    def apply_pending(self, principal: str, request_id: str, digest: str,
                      effect: Callable[[Any], Tuple[int, Any]]) -> Optional[Tuple[int, Any]]:
        """Run a SQLite-only ``effect(connection)`` and store its response atomically.

        Use only for your own ``new`` claim or an explicitly reviewed recovery.
        ``effect`` must use the given connection; it must not commit, roll back
        or close it, and must not call write methods of this same instance
        (that raises GuardError). It returns ``(code, response)``; the response
        is stored as JSON, so later ``response()`` calls return the JSON
        round-trip (tuples become lists) while this call returns the original
        object. Returns None if the request is not pending with this digest.
        """
        require_ids(principal=principal, request_id=request_id, digest=digest)
        with _db.write_txn(self._db):
            row = self._db.execute(
                "SELECT digest, state FROM requests WHERE principal=? AND request_id=?",
                (principal, request_id),
            ).fetchone()
            if not row or tuple(row) != (digest, PENDING):
                return None
            code, response = effect(self._db)
            _require_code(code)
            self._db.execute(
                "UPDATE requests SET state='completed', code=?, response=? "
                "WHERE principal=? AND request_id=? AND digest=? AND state='pending'",
                (code, self._encode(response), principal, request_id, digest),
            )
        return code, response

    def _encode(self, response: Any) -> str:
        encoded = json.dumps(response, ensure_ascii=True, allow_nan=False)
        if self._max_response_bytes is not None and len(encoded) > self._max_response_bytes:
            raise ValueError(f"response is {len(encoded)} bytes as JSON; the limit is {self._max_response_bytes}")
        return encoded

    # -- outbound ---------------------------------------------------------
    def reserve_outbound(self, intent_id: str, fixed: Mapping[str, str]) -> dict:
        """Persist a stable request_id/timestamp for an intent BEFORE sending it.

        Retrying the same ``intent_id`` returns the same context, so the
        receiver sees one request. Reusing it with different ``fixed`` content
        raises IntentMismatch. ``fixed`` is stored verbatim, so never put
        secrets or bodies in it: put a body hash there instead of the body.
        """
        require_ids(intent_id=intent_id)
        if not isinstance(fixed, Mapping) or not all(isinstance(k, str) and isinstance(v, str) for k, v in fixed.items()):
            raise TypeError("fixed must map str to str")
        if {"request_id", "ts"} & set(fixed):
            raise ValueError("fixed must not contain 'request_id' or 'ts'")
        candidate = {**fixed, "request_id": uuid.uuid4().hex, "ts": repr(time.time())}
        with _db.write_txn(self._db):
            self._db.execute(
                "INSERT INTO outbound_requests VALUES (?,?) ON CONFLICT(intent_id) DO NOTHING",
                (intent_id, json.dumps(candidate, sort_keys=True)),
            )
            context = json.loads(self._db.execute(
                "SELECT context FROM outbound_requests WHERE intent_id=?", (intent_id,)).fetchone()[0])
        if any(context.get(k) != v for k, v in fixed.items()) or set(context) - {"request_id", "ts"} != set(fixed):
            raise IntentMismatch(f"intent {intent_id!r} was reserved with different content")
        return context

    def close(self) -> None:
        _db.close(self._db)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

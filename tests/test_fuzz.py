"""Standard-library fuzz test with fixed seeds.

Contract under test:
* every call into a public method of ResourceLock, CircuitBreaker,
  ReplayGuard, or request_digest either succeeds or raises one of the
  documented exceptions (InvalidIdentifier, TypeError, ValueError,
  IntentMismatch, LockHeld, or any other GuardError subclass) - never
  AttributeError, KeyError, IndexError, sqlite3.InterfaceError,
  OverflowError or an undocumented escape;
* after every call the instance is still usable and not inside a
  transaction;
* cli.main(argv) always returns 0, 1 or 2 and lets nothing escape except
  the SystemExit argparse itself raises (which main normally converts to a
  return code).

Randomness: random.Random with fixed seeds, parametrised over three seeds,
so every run is deterministic. The first run found two CLI crashes
(``--older-than nan`` / ``inf``), which were fixed; an unopenable database path
is allowed to raise ``sqlite3.OperationalError`` as documented.
"""
import contextlib
import io
import random
import sqlite3
import tempfile
from pathlib import Path

import pytest

from joons_agent_guard import (
    CircuitBreaker,
    GuardError,
    InvalidIdentifier,
    IntentMismatch,
    LockHeld,
    ReplayGuard,
    ResourceLock,
    request_digest,
)
from joons_agent_guard import cli

SEEDS = (20260101, 424242, 31337)
ITERATIONS = 25
CLI_ITERATIONS = 20

# documented outcomes: success or one of these exception types
DOCUMENTED = (GuardError, TypeError, ValueError)


def _fuzz_value(rng):
    """One randomly generated argument of a random type and size."""
    generators = (
        lambda: None,
        lambda: 0,
        lambda: 1,
        lambda: rng.randint(-10**6, 10**6),
        lambda: True,
        lambda: False,
        lambda: rng.uniform(-1e9, 1e9),
        lambda: float("nan"),
        lambda: float("inf"),
        lambda: -0.0,
        lambda: 0.1 + 0.2,
        lambda: b"payload-" + rng.randbytes(6),
        lambda: b"",
        lambda: "",
        lambda: " " * rng.randint(1, 4),
        lambda: "unicode-π-Ω-데이터",
        lambda: "x" * rng.randint(1, 2000),          # above MAX_IDENTIFIER_CHARS
        lambda: {"k": rng.randint(0, 9), "n": {"deep": ["v", 1, None]}},
        lambda: {1: "int-key", "s": "str-value"},    # non-str dict key
        lambda: [1, "two", None, [3, {"four": 4}]],
        lambda: (1, "two", None),
        lambda: {},
        lambda: [],
        lambda: (),
    )
    return rng.choice(generators)()


def _args(rng, count):
    return [_fuzz_value(rng) for _ in range(count)]


def _assert_documented(label, exc, args, kwargs):
    assert isinstance(exc, DOCUMENTED), (
        f"{label} raised undocumented {type(exc).__name__}: {exc!r} "
        f"(args={args!r}, kwargs={kwargs!r})")


def _assert_usable(kind, instance):
    """After every call: no open transaction, and a clean probe call works."""
    assert not instance._db.in_transaction, f"{kind} left inside a transaction"
    if kind == "lock":
        assert instance.status("probe") is None
    elif kind == "circuit":
        assert instance.snapshot("probe").state in ("closed", "open", "half_open")
    else:
        assert instance.response("probe", "probe", "probe") is None


def _fuzz_calls(kind, instance, callspec, rng, iterations=ITERATIONS):
    """Run randomised calls; anything undocumented fails the assertion."""
    for _ in range(iterations):
        args, kwargs = callspec(rng)
        try:
            getattr(instance, callspec.method)(*args, **kwargs)
            exc = None
        except DOCUMENTED as caught:
            exc = caught
        if exc is not None:
            _assert_documented(f"{kind}.{callspec.method}", exc, args, kwargs)
        _assert_usable(kind, instance)


# -- call specifications ------------------------------------------------------

LOCK_CALLS = {
    "acquire": lambda rng: (_args(rng, 3), {}),
    "release": lambda rng: (_args(rng, 3), {}),
    "status": lambda rng: (_args(rng, 1), {}),
    "break_lock": lambda rng: (_args(rng, 2), {}),
}
CIRCUIT_CALLS = {
    "begin": lambda rng: (_args(rng, 2), {}),
    "finish": lambda rng: (_args(rng, 2),
                           {"success": _fuzz_value(rng), "reason": _fuzz_value(rng)}),
    "snapshot": lambda rng: (_args(rng, 1), {}),
    "reset": lambda rng: (_args(rng, 1), {"expected_job_id": _fuzz_value(rng)}),
}


def _effect(rng):
    kind = rng.randint(0, 3)
    if kind == 0:
        return lambda conn: (200, {"ok": True})
    if kind == 1:
        return lambda conn: (200, None)
    if kind == 2:
        return "not-callable"                       # TypeError when invoked
    return lambda conn: ("500", {"code": "is-a-str"})  # TypeError: code not int


REPLAY_CALLS = {
    "claim": lambda rng: (_args(rng, 3), {}),
    "complete": lambda rng: (_args(rng, 4), {"response": _fuzz_value(rng)}),
    "response": lambda rng: (_args(rng, 3), {}),
    "apply_pending": lambda rng: (_args(rng, 3), {"effect": _effect(rng)}),
    "reserve_outbound": lambda rng: (_args(rng, 1), {"fixed": _fuzz_value(rng)}),
}


def _fuzz_component(kind, factory, callspecs, seed):
    rng = random.Random(seed)
    with tempfile.TemporaryDirectory(prefix="fuzz-") as tmp:
        instance = factory(Path(tmp) / "fuzz.db")
        try:
            for method, spec in callspecs.items():
                spec.method = method
                _fuzz_calls(kind, instance, spec, rng)
        finally:
            instance.close()





# -- the fuzz tests -----------------------------------------------------------

@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_resource_lock(tmp_path, seed):
    _fuzz_component("lock", ResourceLock, LOCK_CALLS, seed)


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_circuit(tmp_path, seed):
    _fuzz_component("circuit", CircuitBreaker, CIRCUIT_CALLS, seed)


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_replay(tmp_path, seed):
    _fuzz_component("replay", ReplayGuard, REPLAY_CALLS, seed)


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_request_digest(seed):
    rng = random.Random(seed)
    bodies = [b"body", b"", bytearray(b"x"), None, 123, {"dict": 1}, "text", 0.5]
    field_sets = [{}, {"k": "v"}, {"k": 1}, {"body_sha256": "x"}, {"n": None},
                  {"unicode": "π"}]
    for _ in range(ITERATIONS):
        method, path = _fuzz_value(rng), _fuzz_value(rng)
        body = rng.choice(bodies)
        fields = rng.choice(field_sets)
        try:
            request_digest(method, path, body, **fields)
            exc = None
        except DOCUMENTED as caught:
            exc = caught
        if exc is not None:
            _assert_documented("request_digest", exc, (method, path, body), fields)


@pytest.mark.parametrize(
    "seed",
    SEEDS)
def test_fuzz_constructors(tmp_path, seed):
    rng = random.Random(seed)
    weird = [None, 123, True, float("nan"), tmp_path, ""]
    for _ in range(6):
        path = rng.choice(weird + [str(tmp_path / "fresh.db")])
        for factory in (ResourceLock, CircuitBreaker, ReplayGuard):
            try:
                instance = factory(path)
            except sqlite3.OperationalError:
                # documented (docs/guarantees.md): an unopenable path such as a
                # directory surfaces as the sqlite3 error itself
                continue
            except DOCUMENTED as caught:
                _assert_documented(f"{factory.__name__}({path!r})", caught, (path,), {})
                continue
            with instance:
                _assert_usable({"ResourceLock": "lock", "CircuitBreaker": "circuit",
                                "ReplayGuard": "replay"}[factory.__name__], instance)


# -- CLI fuzz -----------------------------------------------------------------

TOKENS = [
    "--db", "--json", "--yes", "--by", "--note", "--worker", "--older-than",
    "locks", "circuit", "pending", "break-lock", "reset-circuit",
    "actor-1", "res-1", "job-1", "svc", "req-1", "digest-1",
    "", "-1", "1", "0", " ", "unicode-π", "x" * 50, "--", "-", "not-a-command",
    "x'; DROP TABLE resource_locks--",
]

# deterministic risky invocations (non-finite --older-than used to crash)
TARGETED = [
    ["pending", "--older-than", "nan"],
    ["pending", "--older-than", "inf"],
    ["pending", "--older-than", "1e309"],
    ["pending", "--older-than", "-0.0"],
    ["pending", "--json"],
    ["circuit", "actor-1"],
    ["circuit", ""],
    ["break-lock", "res-1", "--job-id", ""],
    ["break-lock", "x'; DROP", "--job-id", "j", "--yes"],
    ["locks", "--json"],
]


def _populated_db(tmp_path):
    db = tmp_path / "cli.db"
    lock = ResourceLock(db)
    lock.acquire("res-1", "owner-1", "job-1")
    lock.close()
    circuit = CircuitBreaker(db)
    circuit.begin("actor-1", "cjob-1")
    circuit.close()
    guard = ReplayGuard(db)
    digest = "d" * 64
    guard.claim("svc", "req-1", digest)
    guard.complete("svc", "req-1", digest, 200, {"ok": True})
    guard.claim("svc", "req-2", "d2")            # left pending on purpose
    guard.close()
    return db


def _run_cli(argv):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = cli.main(argv)
    return code, buffer.getvalue()


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_cli_random(tmp_path, seed):
    rng = random.Random(seed)
    db = _populated_db(tmp_path)
    for _ in range(CLI_ITERATIONS):
        argv = ["--db", str(db)]
        if rng.random() < 0.8:
            argv = [rng.choice(["--db", str(db)])] + argv[1:]  # keep --db present
        argv += [rng.choice(TOKENS) for _ in range(rng.randint(1, 6))]
        try:
            code = cli.main(argv)
        except SystemExit as exc:              # argparse's own escape: allowed
            assert exc.code in (0, 1, 2)
            continue
        assert code in (0, 1, 2), f"cli.main({argv!r}) returned {code!r}"


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_cli_targeted(tmp_path, seed):
    db = _populated_db(tmp_path)
    rng = random.Random(seed)
    for argv_tail in TARGETED:
        argv = ["--db", str(db)] + argv_tail
        try:
            code = cli.main(argv)
        except SystemExit as exc:
            assert exc.code in (0, 1, 2)
            continue
        assert code in (0, 1, 2), f"cli.main({argv!r}) returned {code!r}"
        _ = rng.random()                       # keep per-seed determinism honest

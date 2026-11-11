# 레시피(Recipes)

`joons_agent_guard`를 위한 실전 패턴 모음입니다. 아래의 모든 코드 조각은 그 자체로 완결된 스크립트입니다. 이 소스 그대로를 대상으로 실행해 확인했습니다(`PYTHONPATH=src python recipe.py` 방식 또는 `pip install .` 이후). 데이터베이스 파일은 임시 디렉터리에 만들어집니다. 공통 규칙은 다음과 같습니다. 아무것도 나이로 만료되지 않습니다. 모든 복구는 명시적인 결정입니다. 모든 상태는 조정 영역(coordination domain)당 SQLite 파일 하나에 살아 있습니다. `apply_pending` 효과는 자기 가드 인스턴스의 쓰기 메서드를 부르지 않습니다(위반하면 `GuardError`가 납니다). 정확한 약속 목록은 `docs/guarantees.md`를 보십시오.

---

## 1. 자기 자신과 겹치지 않는 예약 작업

**문제.** cron과 같은 반복 루프가 같은 작업을 1분마다 시작합니다. 한 번의 실행은 이전 실행과 겹쳐서는 안 됩니다. 프로세스가 다시 시작된 뒤에도 마찬가지입니다. 이전 실행이 도중에 멈췄다면 작업이 조용히 끼어 있어서는 안 됩니다. 누군가 무슨 일이 있었는지 확인한 뒤에 오래된 잠금(lock)을 강제로 풉니다.

```python
"""Recipe 1 - a scheduled job that never overlaps itself, surviving crashes."""
import tempfile
from pathlib import Path

from joons_agent_guard import ResourceLock

db_path = Path(tempfile.mkdtemp()) / "jobs.db"
RESOURCE, OWNER = "nightly-report", "scheduler"
lock = ResourceLock(db_path)


def run_once(job_id):
    """One scheduled run: take the lock, do the work, release it."""
    if not lock.acquire(RESOURCE, OWNER, job_id):
        holder = lock.status(RESOURCE)
        print(f"skip {job_id}: lock held by {holder.owner}/{holder.job_id}")
        return False
    try:
        print(f"{job_id}: doing the work")
        return True
    finally:
        lock.release(RESOURCE, OWNER, job_id)  # also runs when the work raises


run_once("run-1")
run_once("run-2")   # sequential runs are fine: each releases before exiting

# --- a previous run crashed mid-way and never released ----------------------
lock.acquire(RESOURCE, OWNER, "run-3")     # imagine: the process dies right here
run_once("run-4")                          # refused - the stale lock is still held

# --- recovery: verify what happened, then break the stale lock --------------
holder = lock.status(RESOURCE)
print("stuck holder:", holder.owner, "/", holder.job_id)
# Outside this library: check whether run-3's work actually happened.
if holder is not None and holder.job_id == "run-3":
    print("break stale lock:", lock.break_lock(RESOURCE, holder.job_id))
run_once("run-5")                          # succeeds again
```

**주의점**

- 실행마다 새로운 `job_id`를 쓰십시오(시각이나 uuid로 만듭니다). `break_lock`의 안전성은 job-id 일치에서 나옵니다. 재사용한 id는 오래된 강제 해제가 살아 있는 실행을 때리게 만들 수 있습니다.
- 이 잠금은 임대(lease)가 아닙니다. 죽은 잠금자를 알아채는 것은 없습니다. 감지는 자동화하십시오(시작할 때 `status()`를 확인합니다). 그래도 `break_lock`은, 멈춘 실행의 작업이 실제로 있었는지 확인한 뒤에 내는 명시적이고 기록에 남는 결정으로 두십시오.
- 재진입은 없습니다. 같은 프로세스조차 잠금을 쥔 상태에서 두 번째 `acquire`는 `False`를 받습니다.
- 건너뛰기 분기에서 `status()`는 경쟁 상황에서 이론적으로 `None`을 돌려줄 수 있습니다. 여기서는 `acquire()`가 `False`를 돌려주었으므로 그 순간 잠금자가 있었다는 뜻입니다. 그래도 방어적인 코드는 `None`을 처리해야 합니다.

---

## 2. 에이전트 작업자 무리, 각자 실행 자리 하나

**문제.** 수많은 AI 에이전트 작업자가 불안정한 상위 서비스를 향해 작업을 실행합니다. 작업자는 한 번에 하나의 작업을 실행합니다. 실패 뒤에는 회복 시도를 많아야 하나씩 실행합니다. 어느 시도 하나가 성공할 때까지 다른 것은 시작하지 않습니다.

```python
"""Recipe 2 - workers that run one recovery attempt at a time after a failure."""
import tempfile
from pathlib import Path

from joons_agent_guard import CircuitBreaker

breaker = CircuitBreaker(Path(tempfile.mkdtemp()) / "fleet.db")
JOBS = {"job-a": "ok", "job-b": "fail", "job-c": "fail", "job-d": "ok"}


def do_job(job_id):
    if JOBS[job_id] == "fail":
        raise RuntimeError("upstream API returned 500")
    print(f"  ran {job_id}")


def dispatch(actor, job_id):
    if not breaker.begin(actor, job_id):
        print(f"  {actor} busy, pending={breaker.snapshot(actor).pending_job}")
        return False
    try:
        do_job(job_id)
    except Exception as exc:
        breaker.finish(actor, job_id, success=False, reason=str(exc))
        print(f"  {job_id} failed on {actor}")
        return False
    breaker.finish(actor, job_id, success=True)
    return True


ACTOR = "worker-1"
print("state:", breaker.snapshot(ACTOR).state)      # closed
dispatch(ACTOR, "job-a")                             # healthy run
dispatch(ACTOR, "job-b")                             # fails -> open
s = breaker.snapshot(ACTOR)
print("after failure:", s.state, s.consecutive_failures, s.last_failure_reason)

# While open, every start is the single allowed recovery attempt:
assert breaker.begin(ACTOR, "job-c")
print("during recovery:", breaker.snapshot(ACTOR).state)       # half_open
print("second start refused:", breaker.begin(ACTOR, "job-x"))  # False
dispatch(ACTOR, "job-c")                                       # refused while half_open
try:
    do_job("job-c")
except Exception as exc:
    breaker.finish(ACTOR, "job-c", success=False, reason=str(exc))
print("after failed recovery:", breaker.snapshot(ACTOR).state)  # open, failures 2

# The next recovery attempt succeeds and closes the circuit:
assert breaker.begin(ACTOR, "job-d")
do_job("job-d")
breaker.finish(ACTOR, "job-d", success=True)
print("after successful recovery:", breaker.snapshot(ACTOR).state)  # closed
```

**주의점**

- `begin`/`finish`는 하나의 약속입니다. `finish`를 부르지 않는 작업은 작업자를 영원히 끼게 합니다(타임아웃은 없습니다). 작업이 예외를 던져도 `finish`가 실행되게 하십시오.
- 멈춘 작업자는 자신의 자리를 계속 쥡니다. 운영자가 확인한 뒤, 결과를 알면 `finish(actor, job_id, success=...)`를 부르고, 모르면 `reset(actor, job_id)`(또는 `reset(actor)`)를 부릅니다.
- `finish`는 현재 진행 중인 `job_id`에만 성공합니다. 그래서 오래된 작업자가 리셋되었다가 다시 시작된 자리를 마무리할 수 없습니다.
- `open`은 트래픽을 강제로 멈추지 않습니다. `open` 작업자의 모든 `begin`이 곧 회복 시도입니다. 사람이 개입하기까지 진짜로 멈추게 하려면 그 위에 정책 층을 더하십시오(예: `snapshot.consecutive_failures >= N`인 동안 작업 배정을 거절합니다). 그리고 `reset`으로 그 정책을 풉니다.
- `consecutive_failures`와 `reason`은 참고 정보입니다. 쿨다운 타이머는 없습니다.

---

## 3. 멱등(idempotent)한 웹훅 / 결제 끝점

**문제.** 결제 끝점은 같은 `Idempotency-Key` 헤더를 단 재시도를 받습니다. 첫 요청만 결제를 실행합니다. 재시도는 두 번 청구하지 않고 원래 답을 돌려주어야 합니다. 같은 키에 다른 내용이 오면 거절해야 합니다.

```python
"""Recipe 3 - an idempotent payment endpoint keyed by Idempotency-Key."""
import json
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest
from joons_agent_guard.replay import COMPLETED, CONFLICT, NEW, PENDING

guard = ReplayGuard(Path(tempfile.mkdtemp()) / "api.db")
PAYMENTS = []


def payment_endpoint(principal, idempotency_key, method, path, body_bytes):
    """``principal`` comes from the authenticated transport (mTLS, signature),
    never from client-supplied headers."""
    digest = request_digest(method, path, body_bytes)

    verdict = guard.claim(principal, idempotency_key, digest)
    if verdict == NEW:
        payment = {"id": "pay-1", "charged": json.loads(body_bytes)["amount"]}
        PAYMENTS.append(payment)                      # the real-world effect
        guard.complete(principal, idempotency_key, digest, 200, payment)
        return 200, payment
    if verdict == COMPLETED:
        code, body = guard.response(principal, idempotency_key, digest)
        print("  replayed stored answer, no second charge")
        return code, body
    if verdict == PENDING:
        return 409, {"error": "request in progress or interrupted; retry later"}
    return 409, {"error": "Idempotency-Key reused with different content"}  # CONFLICT


body = json.dumps({"amount": 100}).encode()
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", body))
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", body))
print("charges performed:", len(PAYMENTS))

same_key_other_body = json.dumps({"amount": 999}).encode()
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", same_key_other_body))
```

**주의점**

- principal은 신뢰할 수 있는 통신 경로에서 와야 합니다. `Idempotency-Key`나 그 밖의 클라이언트 헤더로 신원을 만들어 내지 마십시오.
- pending은 "결과를 모른다"는 뜻입니다. 409(나중에 다시 시도)로 답하십시오. 효과를 다시 실행하지 마십시오. 작업과 `complete()` 사이에 멈추면 행은 pending으로 남습니다. 레시피 6을 보십시오.
- conflict는 API 관점에서 끝입니다. 같은 키에 다른 내용은 결코 진행되지 않습니다. digest가 의미상 중요한 모든 것을 덮도록 하십시오. `request_digest(method, path, body, **fields)`는 추가 필드(질의 문자열, 임차인 id 등)를 받습니다.
- 행이 그 digest로 pending이 아니면 `complete()`는 `False`를 돌려줍니다. 무시하지 말고 기록으로 남기십시오.
- 저장되는 응답은 JSON으로 바꿀 수 있어야 합니다. 그리고 그대로 재생됩니다. 비밀값을 넣지 마십시오(`SECURITY.md`를 보십시오).

---

## 4. 바깥쪽 호출을 위한 클라이언트 쪽 안전 재시도

**문제.** 서비스가 요청을 보냈는데 응답이 끝내 도착하지 않습니다. 요청은 처리되었을 수도 있고 아니었을 수도 있습니다. 재시도가 몇 번이고 일어나고, 몇 개의 프로세스에서 일어나더라도, 받는 쪽에는 정확히 하나의 고정된 요청 신원이 보여야 합니다.

```python
"""Recipe 4 - client-side retries that can never send two different requests."""
import hashlib
import tempfile
from pathlib import Path

from joons_agent_guard import IntentMismatch, ReplayGuard

db_path = Path(tempfile.mkdtemp()) / "outbound.db"
guard = ReplayGuard(db_path)
INTENT = "create-order-42"
BODY = b'{"amount": 100}'
attempts = {"n": 0}


def send():
    # Reserve BEFORE the first send; every retry gets the same context back.
    ctx = guard.reserve_outbound(INTENT, {
        "method": "POST",
        "path": "/orders",
        "body_sha256": hashlib.sha256(BODY).hexdigest(),
    })
    attempts["n"] += 1
    print(f"  attempt {attempts['n']}: POST /orders, Idempotency-Key={ctx['request_id']}")
    if attempts["n"] == 1:
        raise TimeoutError("no response arrived - outcome unknown")
    return 201, {"order_id": "ord-1"}


def place_order():
    try:
        return send()
    except (TimeoutError, ConnectionError):
        print("  unknown outcome -> retry with the SAME intent")
        return send()


print(place_order())
assert attempts["n"] == 2   # one retry, and both attempts carried one request_id

# Reusing the intent for different content is refused, original stands:
try:
    guard.reserve_outbound(INTENT, {"method": "POST", "path": "/orders", "body_sha256": "other"})
except IntentMismatch as exc:
    print("intent mismatch:", exc)
```

**주의점**

- 첫 보내기 **전에** 예약하십시오. 그 예약이 모든 재시도에 같은 `request_id`를 붙여 줍니다.
- 재시도는 같은 `intent_id`를 재사용해야 합니다. 새 intent id는 새로운 요청 신원입니다.
- `fixed` 값은 문자열이어야 합니다. 본문 **해시**를 저장하지 본문을 저장하지 마십시오. 비밀값은 절대 저장하지 마십시오. `request_id`와 `ts`는 예약된 키입니다(`ValueError`).
- `IntentMismatch`는 그 의도가 이미 다른 내용으로 예약되었다는 뜻입니다. 멈추고 원인을 조사하십시오. 원래 예약은 그대로입니다.
- "많아야 한 번"은 받는 쪽이 당신의 `Idempotency-Key`(`ctx["request_id"]`)로 중복을 제거할 때만 끝까지 성립합니다.
- 예약만 하고 보내지 않은 의도는 쌓입니다(삭제 API가 없습니다). 보존 절차는 밖에서 따로 계획하십시오.

---

## 5. 데이터베이스만의 부수 효과를 정확히 한 번으로 만들기

**문제.** 송금은 행을 옮기면서 그 영수증도 원자적으로 기록해야 합니다. 둘 다 일어나거나 둘 다 일어나지 않아야 합니다. 멈춤과 재시도를 넘어서도 그래야 합니다. 그리고 재시도가 두 번 인출해서는 안 됩니다.

```python
"""Recipe 5 - a money transfer whose effect and receipt commit together."""
import json
import sqlite3
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest

db_path = Path(tempfile.mkdtemp()) / "ledger.db"
guard = ReplayGuard(db_path)

demo_db = sqlite3.connect(db_path)   # application tables live in the same DB file
demo_db.execute("CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER NOT NULL)")
demo_db.execute("INSERT INTO accounts VALUES ('acct-1', 500), ('acct-2', 0)")
demo_db.commit()
demo_db.close()


def transfer(from_id, to_id, amount, request_id):
    body = json.dumps({"from": from_id, "to": to_id, "amount": amount}).encode()
    digest = request_digest("POST", "/transfer", body)

    def effect(conn):
        """SQLite-only: uses the passed connection, never commits itself."""
        conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (amount, from_id))
        conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (amount, to_id))
        left = conn.execute("SELECT balance FROM accounts WHERE id = ?", (from_id,)).fetchone()[0]
        return 200, {"ok": True, "balance_left": left}

    verdict = guard.claim("internal", request_id, digest)
    if verdict == "new":
        return guard.apply_pending("internal", request_id, digest, effect)
    if verdict == "pending":
        # A previous run crashed before commit: nothing was written, so
        # re-running the SQLite-only effect is safe.
        return guard.apply_pending("internal", request_id, digest, effect)
    return guard.response("internal", request_id, digest)   # completed


print(transfer("acct-1", "acct-2", 100, "xfer-1"))   # effect + receipt commit together
print(transfer("acct-1", "acct-2", 100, "xfer-1"))   # retry replays, no double debit
check = sqlite3.connect(db_path)
print(check.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall())
check.close()
```

**주의점**

- `effect`는 SQLite 전용이어야 합니다. 넘겨받은 연결을 쓰십시오. 자체적으로 커밋하거나 롤백하지 마십시오. 같은 가드 인스턴스의 쓰기 메서드를 부르지 마십시오(겹쳐 쓰기는 `GuardError`를 냅니다). 그 안에서 파일이나 네트워크, 서브프로세스 작업을 하지 마십시오. 이것만 지키면 pending 판정에서 다시 실행하는 것이 안전합니다.
- 효과가 예외를 던지면 모든 것이 롤백됩니다. 효과 자신의 쓰기도 포함입니다. 행은 pending으로 남습니다. 예외는 "아무 일도 없었다"로 취급하십시오.
- 응용 표는 가드와 **같은** 데이터베이스 파일에 있어야 합니다. 원자성은 하나의 트랜잭션에서 나옵니다. 트랜잭션은 두 파일에 걸칠 수 없습니다.
- SQLite 밖의 작업에는 `apply_pending`을 쓰지 마십시오. 그 경로에는 원자성이 없습니다. 레시피 3(외부 작업 + `complete`)과 레시피 6(맞추기)의 패턴을 쓰십시오.

---

## 6. 멈춘 뒤 남은 pending 요청 맞추기

**문제.** 한 프로세스가 요청을 차지했고, 외부 작업은 어쩌면 일어났으며, 프로세스는 `complete()` 전에 죽었습니다. 행은 pending입니다. 결과는 모르고 만료도 되지 않습니다. 복구 절차는 이렇습니다. 끼어 있는 행을 읽기 전용으로 찾습니다. 행마다 실제로 무슨 일이 있었는지 정합니다. 그리고 `complete()`로 실제를 기록합니다.

```python
"""Recipe 6 - reconciling requests stuck in pending after a crash."""
import json
import sqlite3
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest

db_path = Path(tempfile.mkdtemp()) / "api.db"
guard = ReplayGuard(db_path)

# --- the crash: claimed, effect ran, complete never happened ----------------
digest = request_digest("POST", "/payments", json.dumps({"amount": 100}).encode())
guard.claim("customer-42", "key-9", digest)      # imagine the process dies here

# --- step 1: read-only inspection (startup sweep or operator script) --------
ro = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
stuck = ro.execute(
    "SELECT principal, request_id, digest, state FROM requests WHERE state='pending'"
).fetchall()
ro.close()
for principal, request_id, _stored_digest, _state in stuck:
    print("stuck pending:", principal, request_id)


def verified_downstream(principal, request_id):
    """Your out-of-band check: provider records, bank statement, logs.
    Returns True for this demo."""
    return True


# --- step 2: decide per row, then record reality with complete() ------------
for principal, request_id, stored_digest, _state in stuck:
    if verified_downstream(principal, request_id):
        guard.complete(principal, request_id, stored_digest, 200, {"status": "paid"})
        print("reconciled:", request_id)

# --- step 3: reconciled rows replay like any completed request --------------
print(guard.response("customer-42", "key-9", digest))
```

**주의점**

- **읽기 전용** 연결로 들여다보십시오. 그리고 `requests` 표를 손으로 고치지 마십시오. 행을 앞으로 옮길 수 있는 것은 `complete()`/`apply_pending()`뿐입니다. 둘은 digest를 다시 확인하기 때문입니다.
- `complete()`에는 저장된 digest가 필요합니다. 행에서 가져오십시오. 직접 다시 계산해도 안전한 경우는 digest 입력이 결정적이고 변하지 않았을 때뿐입니다.
- 행마다 정하십시오. 작업이 있었으면 실제 결과로 `complete()`를 부릅니다. 없었으면 실행한 뒤 `complete()`를 부릅니다. 알 수 없고 작업이 바깥 세상에 손을 댔다면 pending으로 두고 위로 올리십시오. 작업이 SQLite 전용이었던 행은 `apply_pending`으로 이어 갈 수 있습니다(레시피 5).
- 행을 지우는 API는 없습니다. pending/conflict 행은 맞춰질 때까지 남습니다. 완료된 행은 쌓입니다. 보존 절차는 밖에서 따로 계획하십시오.

## 쇼케이스(Showcase)

`examples/agent_fleet_demo.py`는 세 구성 요소를 하나의 데이터베이스 파일 위에서 함께 쓰는 종합 쇼케이스입니다. asyncio로 이루어진 네 작업자 무리가 각자 자신의 `AsyncCircuitBreaker` 실행 자리를 쓰며 열두 개의 파일 편집 작업을 처리합니다. 모든 작업은 먼저 대상 파일에 대한 `AsyncResourceLock`을 잠급니다. 두 작업이 하나의 대상을 공유하므로 편집은 한 줄로 섭니다. 기다리던 잠금 시도는 잠금 대기로 집계됩니다. 모든 제출은 `request_digest`와 함께 `AsyncReplayGuard`를 거칩니다. 끝에 일부러 중복 제출한 하나는 두 번 실행되는 대신 저장된 응답으로 답을 받습니다. 한 작업자는 첫 두 번의 시도를 일부러 실패하도록 만들어져 있습니다. 그래서 기록을 보면 이 작업자의 회로가 `open -> half_open -> open -> half_open -> closed`로 변하고, 실패한 작업들은 다시 줄에 들어가더라도 다른 곳에서 여전히 정확히 한 번만 실행됩니다. 스크립트는 연결이 열려 있는 동안 불변식을 확인합니다. 모든 작업은 정확히 한 번 편집됩니다. 하나의 대상에 겹치는 편집은 없습니다. 중복은 두 번 실행 없이 재생됩니다. 마지막에 회로 상태들을 확인합니다. 스크립트는 0으로 끝나며, 전체 실행은 1초도 걸리지 않습니다.

    PYTHONPATH=src python examples/agent_fleet_demo.py

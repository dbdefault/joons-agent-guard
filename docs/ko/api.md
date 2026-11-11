# API 참조 — joons_agent_guard

English original: [docs/api.md](../api.md)

문서화한 버전은 0.1.0입니다(`joons_agent_guard.__version__`). 이 참조 문서는 `src/joons_agent_guard/` 아래의 소스를 읽고 작성했습니다. 모든 시그니처(signature)는 `inspect.signature`가 출력하는 그대로입니다. 즉 `X | None`은 `Optional[X]`를 뜻하고, 데이터 클래스(dataclass) 반환 타입은 전체 경로로 표시됩니다. 문서화된 이름의 목록은 정확히 `joons_agent_guard.__all__`과 같습니다. 모든 예제는 그 자체로 완결되어 혼자서 실행됩니다. 각 예제는 자신만의 짧은 임시 데이터베이스를 씁니다. 따라서 디스크에 아무것도 남기지 않습니다.

## 세 구성 요소에 공통인 규칙

- **저장.** 각 구성 요소는 `path`로 주어진 파일에 대한 `sqlite3` 연결 하나를 감쌉니다. `path`는 `str`이나 경로 같은 객체, 또는 `':memory:'`입니다. 세 구성 요소는 서로 다른 표를 씁니다. 그래서 하나의 데이터베이스 파일을 함께 쓸 수 있습니다. 모든 쓰기는 명시적인 `BEGIN IMMEDIATE ... COMMIT` 트랜잭션(transaction) 안에서 실행됩니다. 그래서 같은 컴퓨터의 여러 프로세스는 SQLite 파일 잠금으로 한 줄로 섭니다. 인스턴스 하나를 여러 스레드가 함께 쓸 수도 있습니다. 하나의 연결은 재진입 잠금(re-entrant lock)이 직렬화하기 때문입니다.
- **`timeout`**(생성자 키워드, 초)은 SQLite의 바쁨 대기 시간(busy timeout)입니다. 다른 프로세스의 쓰기 트랜잭션을 이만큼 기다린 뒤 `sqlite3.OperationalError: database is locked`를 냅니다. 이 값은 잠금이나 처리 중 요청이나 회로 자리의 만료(expiry)가 아닙니다. 이들은 스스로 시간이 다해 끝나는 일이 없습니다.
- **식별자(identifier)**(`resource`, `owner`, `job_id`, `actor`, `principal`, `request_id`, `digest`, `intent_id`, ...)은 비어 있지 않은 `str`이면서 최대 1024자(`MAX_IDENTIFIER_CHARS`) 이하이어야 합니다. 그렇지 않으면 `InvalidIdentifier`가 발생합니다.
- **수명 주기.** 모든 구성 요소는 컨텍스트 관리자(context manager) 프로토콜을 지원합니다. `with ResourceLock(path) as lock: ...`은 블록이 끝날 때 연결을 닫습니다. 또한 `close()` 메서드가 있습니다. 닫힌 뒤에 구성 요소를 쓰면 `sqlite3.ProgrammingError`가 납니다.
- **SQLite 오류.** 어떤 `sqlite3.Error`든 데이터베이스를 만지는 어떤 메서드에서든 나올 수 있습니다. 바쁜 데이터베이스나 가득 찬 디스크에서의 `OperationalError`가 그 예입니다. 트랜잭션은 오류가 나오기 전에 롤백(rollback)됩니다.

## 예외

모든 패키지 오류는 `GuardError`를 물려받습니다.

| 예외 | 기반이 되는 클래스 | 내는 곳 |
| --- | --- | --- |
| `GuardError` | `Exception` | 기반 클래스입니다. `apply_pending` 효과가 자기를 실행 중인 인스턴스에 겹쳐 쓰기를 하면 이 예외가 직접 납니다 |
| `InvalidIdentifier` | `GuardError`, `ValueError` | 비어 있지 않은 `str`이 아닌 식별자를 받은 모든 메서드 |
| `LockHeld` | `GuardError` | 자원이 이미 잠겨 있을 때 `ResourceLock.hold`가 냅니다(속성은 `resource`와 `holder`이며, `holder`는 현재 잠금자의 `LockInfo` 또는 `None`입니다) |
| `IntentMismatch` | `GuardError`, `ValueError` | 같은 의도(intent) id를 다른 내용으로 재사용할 때 `ReplayGuard.reserve_outbound`가 냅니다 |
| `CorruptStorage` | `GuardError` | 저장된 응답을 JSON으로 해독할 수 없을 때(망가졌거나 조작된 데이터베이스 파일) `ReplayGuard.response`가 냅니다 |

`InvalidIdentifier`와 `IntentMismatch`는 `ValueError`이기도 합니다. 따라서 평범한 `except ValueError`가 이들을 잡습니다. `GuardError`, `InvalidIdentifier`, `IntentMismatch`, `CorruptStorage`는 기본 `Exception` 생성자를 씁니다. 그래서 `inspect.signature`가 이들을 위해 출력할 것이 없습니다. `LockHeld`에는 `LockHeld(resource, holder=None)`이 있습니다. 이 값은 `hold`가 자동으로 채웁니다. 속성은 `exc.resource`와 `exc.holder`입니다.

```python
import tempfile

from joons_agent_guard import LockHeld, ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("deploy-api", "worker-0", "job-41")    # another holder got there first
    try:
        with lock.hold("deploy-api", owner="worker-1", job_id="job-42"):
            pass                                        # runs only if the lock was free
    except LockHeld as exc:
        print(exc.holder.owner, "has held", exc.resource)   # worker-0
    lock.close()
```

---

## request_digest

```text
request_digest(method: str, path: str, body: bytes, **fields: str) -> str
```

요청의 정규화된 다이제스트(digest)를 계산합니다. 이것은 정렬된 키의 JSON 문서를 SHA-256으로 해시한 값입니다. 그 문서에는 `method`와 `path`, 본문의 SHA-256(`body_sha256`으로), 키워드로 넘긴 모든 추가 필드가 들어갑니다. 결과는 프로세스와 파이썬 버전을 넘나들어도 같습니다. 같은 값을 `ReplayGuard`의 `claim`, `complete`, `response`, `apply_pending`에서 `digest` 인자로 쓰십시오. 작업의 의미를 바꾸는 모든 것을 넣으십시오. 그리고 비밀값은 넣지 마십시오. 다이제스트는 비밀값이 아니지만 암호화도 되지 않습니다.

**매개변수.** `method` — HTTP 방식의 동사입니다. `path` — 요청 경로입니다. `body` — 원본 요청 본문입니다. `bytes`나 `bytearray`입니다. `**fields` — 추가 문자열 필드입니다(키 순서는 상관없습니다).

**반환값.** 64글자의 소문자 16진수 문자열입니다.

**예외.** `body`가 bytes/bytearray가 아니거나 어떤 추가 필드 값이 `str`이 아니면 `TypeError`입니다. 추가 필드의 이름이 `body_sha256`이면(예약됨) `ValueError`입니다.

```python
from joons_agent_guard import request_digest

d1 = request_digest("POST", "/charges", b'{"order": 7}', tenant="t1")
d2 = request_digest("POST", "/charges", b'{"order": 7}', tenant="t1")
assert d1 == d2 and len(d1) == 64          # stable, 64 hex chars
assert d1 != request_digest("POST", "/charges", b'{"order": 8}', tenant="t1")
```

---

## LockInfo

```text
LockInfo(resource: str, owner: str, job_id: str, acquired_at: float) -> None
```

하나의 잡힌 잠금을 서술하는 불변 데이터 클래스(dataclass)입니다. 보통은 직접 만들지 않습니다. `ResourceLock.status`와 `ResourceLock.list_locks`, `LockHeld.holder`에서 인스턴스를 받습니다.

**필드.** `resource` — 잠긴 자원의 이름입니다. `owner` — 잠금자의 신원입니다. `job_id` — 잠금자의 작업입니다. `acquired_at` — 획득 시각이며 `time.time()` 형식의 실수입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    assert lock.acquire("deploy-api", "worker-1", "job-42")
    info = lock.status("deploy-api")
    print(info.owner, info.job_id, info.acquired_at)   # worker-1 job-42 <epoch>
    lock.close()
```

---

## ResourceLock

```text
ResourceLock(path, *, timeout: float = 10.0)
```

이름 붙인 문자열 자원에 대한 배타적 잠금(lock)입니다. 같은 데이터베이스 파일을 여는 모든 프로세스와 모든 스레드가 함께 봅니다. 잠금은 `(owner, job_id)` 쌍이 쥡니다. 잠금은 프로세스가 다시 시작되는 것을 넘어 살아 남습니다. 스스로 만료되지는 않습니다. 잠금자가 멈췄다면 명시적이고 감사되는 `break_lock`만 잠금을 제거합니다. 이 구성 요소를 열면 필요할 때 `resource_locks` 표를 만듭니다.

**매개변수.** `path` — SQLite 데이터베이스 파일입니다. `timeout` — 바쁨 대기 시간(초)이며 기본값은 10.0입니다.

**예외.** 파일을 열 수 없으면 `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    with ResourceLock(tmp + "/guard.db") as lock:      # closed automatically
        if not lock.acquire("deploy-api", "worker-1", "job-42"):
            print(lock.status("deploy-api"))           # who is holding it
```

### `ResourceLock.acquire(resource: str, owner: str, job_id: str) -> bool`

자원 `resource`에 대한 배타적 잠금을 `(owner, job_id)` 잠금자가 가져갑니다. 이미 누군가 — 같은 잠금자라도 — 잠금을 쥐고 있으면 막지 않고 실패합니다. 대기 줄이 없으므로 호출자가 다시 시도하거나 물러섭니다. 행에 기록되는 획득 시각은 명령줄 도구가 보여 주는 `age`의 근거입니다.

**매개변수.** `resource` — 잠글 자원의 이름입니다. `owner` — 호출자의 안정적인 신원입니다(프로세스/서비스). `job_id` — 이번 실행의 식별자입니다.

**반환값.** 이 호출이 잠금을 가져갔으면 `True`입니다. 이미 잠겨 있으면 `False`입니다.

**예외.** `InvalidIdentifier`입니다. 바쁘거나 쓸 수 없는 데이터베이스에서는 `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    if lock.acquire("deploy-api", owner="worker-1", job_id="job-42"):
        try:
            print("deploying")                 # the real work goes here
        finally:
            lock.release("deploy-api", owner="worker-1", job_id="job-42")
    lock.close()
```

### `ResourceLock.release(resource: str, owner: str, job_id: str) -> bool`

잠금을 놓습니다. `owner`와 `job_id`가 모두 현재 잠금자와 맞을 때만 놓습니다. 낡은 작업이 새 작업이 다시 잡은 잠금을 놓을 수 없습니다.

**매개변수.** `resource`, `owner`, `job_id` — 현재 잠금자를 정확히 가리켜야 합니다.

**반환값.** 맞는 잠금을 제거했으면 `True`입니다. 아니면 `False`입니다(맞는 잠금자가 없습니다. 잠금 상태는 변하지 않습니다).

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("deploy-api", "worker-1", "job-42")
    assert not lock.release("deploy-api", "worker-1", "other-job")  # refused
    assert lock.release("deploy-api", "worker-1", "job-42")         # released
    lock.close()
```

### `ResourceLock.status(resource: str) -> joons_agent_guard.lock.LockInfo | None`

현재 `resource`를 누가 쥐고 있는지 돌려줍니다. 운영자가 판단하기 전에 확인하는 용도입니다. 읽기는 쓰기를 오래 막지 않습니다. 그리고 상태를 바꾸지도 않습니다.

**매개변수.** `resource` — 들여다볼 자원의 이름입니다.

**반환값.** `LockInfo`(resource, owner, job_id, acquired_at)이거나 `None`입니다. 자원이 잠기지 않았으면 `None`입니다.

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile
import time

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("deploy-api", "worker-1", "job-42")
    info = lock.status("deploy-api")
    age = time.time() - info.acquired_at        # how long it has been held
    lock.close()
```

### `ResourceLock.list_locks() -> List[joons_agent_guard.lock.LockInfo]`

데이터베이스에 지금 잡힌 모든 잠금을 돌려줍니다. 획득 시각 오름차순으로 정렬합니다. 모든 잠금자를 아우르는 감사 화면입니다.

**매개변수.** 없습니다.

**반환값.** `LockInfo`의 목록이며 가장 오래된 획득이 먼저입니다(아무것도 잠기지 않았으면 비어 있습니다).

**예외.** `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("a", "owner-1", "job-1")
    lock.acquire("b", "owner-2", "job-2")
    for info in lock.list_locks():
        print(info.resource, info.owner, info.job_id)
    lock.close()
```

### `ResourceLock.break_lock(resource: str, expected_job_id: str) -> bool`

죽은 것으로 확인된 잠금자를 위한 운영자 복구입니다. 기록된 `job_id`가 아직 `expected_job_id`와 같을 때만 잠금을 제거합니다. 그 사이에 새 작업이 잠금을 다시 잡았다면 그 잠금은 지워지지 않습니다. 먼저 `status()`로 확인하고, 잠금자가 정말 사라졌음을 확인한 뒤에 해제하십시오. 놓인 자원은 곧바로 다시 잠을 수 있습니다.

**매개변수.** `resource` — 잠긴 자원입니다. `expected_job_id` — 죽었다고 믿는 작업의 id입니다.

**반환값.** 맞는 잠금을 제거했으면 `True`입니다. 잠금이 없거나 다른 작업이 쥐고 있으면 `False`입니다.

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("deploy-api", "worker-1", "job-42")    # holder died; operator checked
    assert lock.break_lock("deploy-api", expected_job_id="job-42")
    assert lock.acquire("deploy-api", "worker-2", "job-43")
    lock.close()
```

### `ResourceLock.hold(resource: str, owner: str, job_id: str)`

acquire/release의 컨텍스트 관리자 형태입니다. 들어가며 잠그거나 `LockHeld`를 냅니다. 자원이 바쁘면 `LockHeld`가 납니다. 이 예외는 현재 잠금자의 `LockInfo`를 `holder` 속성으로 갖고 있습니다. 나올 때는 항상 놓습니다. 본문이 예외를 던져도 마찬가지입니다. 자원이 잡혔을 때만 본문이 실행됩니다.

**매개변수.** `resource`, `owner`, `job_id` — `acquire`와 같습니다.

**반환값.** 아무것도 넘겨주지 않는 컨텍스트 관리자입니다.

**예외.** `InvalidIdentifier`입니다. 바쁜 자원에서는 `LockHeld`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import LockHeld, ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    lock.acquire("deploy-api", "worker-0", "job-41")
    try:
        with lock.hold("deploy-api", owner="worker-1", job_id="job-42"):
            print("deploying")                  # runs only when the lock was free
    except LockHeld as exc:
        print("busy:", exc.holder.owner)        # worker-0 holds it
    lock.close()
```

### `ResourceLock.close() -> None`

내부의 SQLite 연결을 닫습니다. 같은 인스턴스의 다른 스레드가 진행 중인 트랜잭션이라면 끝날 때까지 기다립니다. 이 뒤에는 모든 메서드가 `sqlite3.ProgrammingError`를 냅니다. 보통의 방식은 이것을 자동으로 부르는 컨텍스트 관리자 형태입니다.

**매개변수.** 없습니다. **반환값.** 없습니다.

**예외.** 닫기가 실패하면 `sqlite3.Error`입니다.

```python
import tempfile

from joons_agent_guard import ResourceLock

with tempfile.TemporaryDirectory() as tmp:
    lock = ResourceLock(tmp + "/guard.db")
    try:
        lock.acquire("deploy-api", "worker-1", "job-42")
    finally:
        lock.close()                            # connection really closed
```

---

## CircuitState

```text
CircuitState(actor: str, state: str, pending_job: str | None, consecutive_failures: int, last_failure_reason: str | None) -> None
```

`CircuitBreaker.snapshot`이 돌려주는 불변 데이터 클래스입니다. 보통은 읽기만 하고 직접 만들지 않습니다.

**필드.** `actor` — 이 상태가 속한 작업자(actor)입니다. `state` — `closed`, `open`, `half_open` 가운데 하나입니다. `pending_job` — 지금 실행 자리를 쥔 작업의 id이거나 `None`입니다. `consecutive_failures` — 마지막 성공/리셋 이후의 실패 횟수입니다. `last_failure_reason` — 마지막 실패의 이유 문자열이거나 `None`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    snap = breaker.snapshot("deploy-worker")
    print(snap.state, snap.pending_job, snap.consecutive_failures)
    breaker.close()
```

---

## CircuitBreaker

```text
CircuitBreaker(path, *, timeout: float = 10.0)
```

작업자(actor)마다 실행 자리(run slot) 하나를 두는 영속적인 회로 차단기입니다. 각 actor는 세 상태 가운데 하나입니다. `closed`는 건강하며 다음 작업이 시작될 수 있다는 뜻입니다. `open`은 마지막으로 끝난 작업이 실패했다는 뜻입니다. 다음으로 시작하는 작업이 회복 시도입니다. `half_open`은 회복 시도가 진행 중이라는 뜻입니다. 다른 것은 시작할 수 없습니다. 시작만 하고 끝내지 못한 작업은 자리를 영원히 쥡니다. 나이로 지워지지 않습니다. 명시적으로 끝내거나 운영자가 `reset`을 부를 때까지입니다. 이 구성 요소를 열면 필요할 때 `worker_circuits` 표를 만듭니다.

**매개변수.** `path` — SQLite 데이터베이스 파일입니다. `timeout` — 바쁨 대기 시간(초)이며 기본값은 10.0입니다.

**예외.** 파일을 열 수 없으면 `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    with CircuitBreaker(tmp + "/guard.db") as breaker:
        if breaker.begin("deploy-worker", job_id="job-42"):
            breaker.finish("deploy-worker", "job-42", success=True)
```

### `CircuitBreaker.begin(actor: str, job_id: str) -> bool`

actor의 단 하나의 실행 자리를 `job_id`를 위해 차지합니다. 다른 작업이 진행 중이 아닐 때만 성공합니다. 작업이 진행 중이거나 actor가 `half_open`이면 다른 모든 `begin`은 `False`를 돌려줍니다. actor가 `open`일 때 성공한 `begin`은 회복 시도이며 상태는 `half_open`이 됩니다(실패 수와 이유는 유지됩니다). `closed`에서는 상태가 `closed`인 채로 자리만 잡힙니다.

**매개변수.** `actor` — 작업자의 신원입니다. `job_id` — 이제 실행할 작업의 식별자입니다.

**반환값.** 자리를 차지했으면 `True`입니다. 다른 작업이 쥐고 있으면 `False`입니다.

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    if breaker.begin("deploy-worker", "job-42"):
        print("running job-42")                 # the only job for this actor
    else:
        print("slot busy:", breaker.snapshot("deploy-worker").pending_job)
    breaker.close()
```

### `CircuitBreaker.finish(actor: str, job_id: str, *, success: bool, reason: str | None = None) -> bool`

진행 중인 작업의 결과를 기록하고 자리를 놓습니다. 성공이면 actor는 `closed`로 돌아가고 실패 수와 이유가 지워집니다. 실패면 actor는 `open`이 되고 `consecutive_failures`가 하나 올라가며 `last_failure_reason`이 저장됩니다. 진행 중인 `job_id`만 끝낼 수 있습니다. 다른 것은 아무것도 바꾸지 않습니다.

**매개변수.** `actor` — 작업자입니다. `job_id` — 진행 중인 작업이어야 합니다. `success` — 결과입니다. `reason` — 실패의 설명입니다(성공에서는 무시됩니다). 4096자(`MAX_REASON_CHARS`)보다 긴 이유는 저장되기 전에 잘립니다.

**반환값.** 결과를 기록했으면 `True`입니다. `job_id`가 진행 중인 작업이 아니면 `False`입니다(상태는 변하지 않습니다).

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    breaker.begin("deploy-worker", "job-42")
    try:
        print("deploying")                      # the real job; raise to fail it
        breaker.finish("deploy-worker", "job-42", success=True)
    except Exception as exc:
        breaker.finish("deploy-worker", "job-42", success=False, reason=str(exc))
    breaker.close()
```

### `CircuitBreaker.snapshot(actor: str) -> joons_agent_guard.circuit.CircuitState`

actor의 현재 상태와 진행 중인 작업과 연속 실패 수와 마지막 실패 이유를 읽습니다. 대시보드와 시작 여부를 정하는 데 쓰는 건강 화면입니다.

**매개변수.** `actor` — 들여다볼 작업자입니다.

**반환값.** `CircuitState`입니다. 한 번도 본 적 없는 actor는 `closed`와 빈 자리와 0의 실패로 보고합니다.

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    snap = breaker.snapshot("deploy-worker")
    if snap.state == "half_open":
        print("recovery attempt in flight:", snap.pending_job)
    breaker.close()
```

### `CircuitBreaker.reset(actor: str, expected_job_id: str | None = None) -> bool`

운영자 복구입니다. actor를 `closed`로 지웁니다(자리를 놓고, 실패 수를 0으로 하고, 이유를 지웁니다). 끝내지 못한 작업에 무슨 일이 있었는지 확인한 뒤에 쓰십시오. `expected_job_id`를 주면 그 작업이 아직 진행 중일 때만 리셋이 적용됩니다. 다른 작업이 대신 자리를 잡은 경우를 막는 안전장치입니다.

**매개변수.** `actor` — 지울 작업자입니다. `expected_job_id` — 선택적이며, 이 작업이 아직 진행 중일 때만 리셋이 적용됩니다.

**반환값.** actor를 리셋했으면 `True`입니다. 맞는 것이 없으면 `False`입니다(모르는 actor이거나 다른 작업이 진행 중인 경우).

**예외.** `InvalidIdentifier`입니다(`actor`에 대해, 또는 비어 있지 않은 잘못된 `expected_job_id`에 대해). `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    breaker.begin("deploy-worker", "job-42")    # the job crashed without finishing
    snap = breaker.snapshot("deploy-worker")    # operator checked: holder gone
    assert breaker.reset("deploy-worker", expected_job_id=snap.pending_job)
    assert breaker.snapshot("deploy-worker").state == "closed"
    breaker.close()
```

### `CircuitBreaker.close() -> None`

내부의 SQLite 연결을 닫습니다. 이 뒤에는 모든 메서드가 `sqlite3.ProgrammingError`를 냅니다. 컨텍스트 관리자 형태가 자동으로 부릅니다.

**매개변수.** 없습니다. **반환값.** 없습니다. **예외.** 닫기가 실패하면 `sqlite3.Error`입니다.

```python
import tempfile

from joons_agent_guard import CircuitBreaker

with tempfile.TemporaryDirectory() as tmp:
    breaker = CircuitBreaker(tmp + "/guard.db")
    try:
        breaker.begin("deploy-worker", "job-42")
    finally:
        breaker.close()
```

---

## ReplayGuard

```text
ReplayGuard(path, *, timeout: float = 15.0, max_response_bytes: int | None = 1048576)
```

재시도되는 요청의 영속적인 많아야 한 번(at-most-once) 처리입니다. 키는 `(principal, request_id)`이고 내용의 `digest`가 함께합니다. `claim`은 키마다 정확히 한 번 `new`를 돌려줍니다. 차지된 요청은 `complete`나 `apply_pending`이 결과를 기록할 때까지 `pending`으로 남습니다. pending은 *모른다*는 뜻입니다. 멈춘 시도의 결과를 알 수 없으므로 나이로 다시 실행하거나 풀지 않습니다. 완료된 키는 저장된 응답을 재생(replay)합니다. 같은 키에 다른 digest는 `conflict`입니다. 이 가드는 누구도 인증하지 않습니다. `principal`은 신뢰할 수 있는 통신 경로에서 와야 합니다. 클라이언트가 넣은 헤더에서 오면 안 됩니다. 이 구성 요소를 열면 필요할 때 `requests` 표와 `outbound_requests` 표를 만듭니다.

**매개변수.** `path` — SQLite 데이터베이스 파일입니다. `timeout` — 바쁨 대기 시간(초)이며 기본값은 15.0입니다. 다른 구성 요소보다 큽니다. `apply_pending` 효과가 쓰기 트랜잭션 안에서 실행되기 때문입니다. `max_response_bytes` — 하나의 저장되는 응답을 JSON으로 바꾼 크기의 상한입니다(바이트). 기본값은 1048576, 즉 1 MiB입니다. `None`이면 상한이 없습니다.

**예외.** 파일을 열 수 없으면 `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

with tempfile.TemporaryDirectory() as tmp:
    with ReplayGuard(tmp + "/guard.db") as guard:
        digest = request_digest("POST", "/charges", b'{"order": 7}')
        if guard.claim("client-a", "req-1", digest) == "new":
            guard.complete("client-a", "req-1", digest, 200, {"charged": True})
```

### `ReplayGuard.claim(principal: str, request_id: str, digest: str) -> str`

많아야 한 번의 관문입니다. `(principal, request_id)` 쌍에 대한 첫 호출은 요청을 `pending`으로 삽입하고 claim 시각을 기록합니다. 그리고 `new`를 돌려줍니다. 일을 실행하십시오. 이후 같은 digest의 호출은 `pending`을 돌려줍니다. 이전 시도가 끝나지 않았다는 뜻이므로 "나중에 다시 시도"로 답하고 다시 실행하지 않습니다. 완료 후에는 `completed`를 돌려줍니다. 저장된 응답을 재생하라는 뜻입니다. 같은 쌍에 다른 digest는 `conflict`를 돌려줍니다. 키가 다른 일에 재사용되었다는 뜻이며, 원래 차지는 그대로입니다.

**매개변수.** `principal` — 인증된 호출자의 신원입니다(이름 공간). `request_id` — 멱등 키이며 한 논리적 작업의 모든 재시도에서 같습니다. `digest` — 요청의 `request_digest`입니다.

**반환값.** `"new"`, `"pending"`, `"completed"`, `"conflict"` 가운데 하나입니다.

**예외.** `InvalidIdentifier`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    digest = request_digest("POST", "/charges", b'{"order": 7}')
    state = guard.claim("client-a", "req-1", digest)
    if state == "new":
        guard.complete("client-a", "req-1", digest, 200, {"charged": True})
    print(state)                                # "new": run it exactly once
    guard.close()
```

### `ReplayGuard.complete(principal: str, request_id: str, digest: str, code: int, response: Any) -> bool`

진행 중인 요청의 결과를 저장합니다. 상태는 `completed`가 되고 주어진 상태 `code`와 JSON으로 바꾼 `response`가 저장됩니다. 맞는 digest를 가진 pending 요청만 완료될 수 있습니다. 그리고 한 번만 완료됩니다. 두 번째 호출은 실패합니다. 따라서 첫 응답이 언제나 이깁니다. 응답은 JSON으로 저장됩니다. 튜플은 목록으로 돌아오고 문자열이 아닌 사전 키는 문자열로 돌아옵니다. JSON에서 안정적인 타입을 쓰십시오. `NaN`과 `Infinity`는 거절됩니다. 그리고 JSON 인코딩은 `max_response_bytes` 안에 들어가야 합니다.

**매개변수.** `principal`, `request_id`, `digest` — 진행 중인 차지를 가리킵니다. `code` — 응답과 함께 저장되는 정수 상태 코드입니다. `response` — JSON으로 바꿀 수 있는 임의의 객체입니다.

**반환값.** 응답을 저장했으면 `True`입니다. 요청이 이 digest로 pending이 아니면 `False`입니다(이미 완료됨, 다른 digest가 차지함, 차지된 적 없음).

**예외.** `InvalidIdentifier`입니다. `code`가 `int`가 아니면(`bool`은 거절됩니다) 또는 `response`를 JSON으로 바꿀 수 없으면 `TypeError`입니다. 응답이 `NaN`/`Infinity`를 담거나 JSON 인코딩이 `max_response_bytes`를 넘으면 `ValueError`입니다(트랜잭션이 열리기 전에 검사합니다). `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    digest = request_digest("POST", "/charges", b'{"order": 7}')
    assert guard.claim("client-a", "req-1", digest) == "new"
    assert guard.complete("client-a", "req-1", digest, 200, {"charged": True})
    assert not guard.complete("client-a", "req-1", digest, 500, {"error": 1})
    assert guard.response("client-a", "req-1", digest) == (200, {"charged": True})
    guard.close()
```

### `ReplayGuard.response(principal: str, request_id: str, digest: str) -> Tuple[int, Any] | None`

완료된 요청의 저장된 응답을 다시 읽습니다. 재시도가 작업을 다시 실행하는 대신 돌려주어야 하는 값입니다. 맞는 digest를 가진 완료된 행만 돌려줍니다. 진행 중이거나 충돌하는 digest는 아무것도 보지 못합니다. 저장된 응답을 JSON으로 해독할 수 없으면 `CorruptStorage`가 납니다.

**매개변수.** `principal`, `request_id`, `digest` — 완료된 요청을 가리킵니다.

**반환값.** `(code, response)` 쌍이거나 `None`입니다. 이 digest에 대해 저장된 응답이 없으면 `None`입니다(차지된 적 없음, 아직 진행 중, 다른 digest).

**예외.** `InvalidIdentifier`입니다. 저장된 응답을 해독할 수 없으면 `CorruptStorage`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    digest = request_digest("POST", "/charges", b'{"order": 7}')
    if guard.claim("client-a", "req-1", digest) == "completed":
        code, payload = guard.response("client-a", "req-1", digest)
    guard.close()
```

### `ReplayGuard.apply_pending(principal: str, request_id: str, digest: str, effect: Callable[[Any], Tuple[int, Any]]) -> Tuple[int, Any] | None`

SQLite 전용 효과(effect)를 실행하고 그 응답을 같은 트랜잭션 안에 저장합니다. 정확히 한 번(exactly-once) 경로입니다. `effect(connection)`은 쓰기 트랜잭션 안에서 가드 자신의 연결을 받습니다. 자기 SQL은 그 연결로 실행해야 합니다. 스스로 커밋하거나 롤백하거나 닫으면 안 됩니다. 같은 가드 인스턴스의 쓰기 메서드를 부르면 안 됩니다(그러면 `GuardError`가 납니다). 효과는 `(code, response)` 쌍을 돌려주어야 합니다. 요청의 완료 표시와 응답 저장은 효과의 쓰기와 같이 커밋됩니다. 그래서 멈춤이나 실패하는 효과는 요청을 pending으로 남기고 효과를 롤백합니다. 자기 `new` 차지에 쓰거나 명시적으로 검토된 pending 요청의 복구에만 쓰십시오. 파일, 네트워크, 서브프로세스 작업에는 절대 쓰지 마십시오. 그런 작업은 저장된 응답과 원자적일 수 없습니다.

**매개변수.** `principal`, `request_id`, `digest` — 진행 중인 차지를 가리킵니다. `effect` — `(connection) -> (code, response)`인 호출 가능 객체입니다. `code`는 `int`여야 합니다(`bool`은 거절됩니다).

**반환값.** 효과가 돌려준 `(code, response)` 쌍입니다(원본 객체입니다). 요청이 이 digest로 pending이 아니면 `None`입니다. 이때 효과는 실행되지 않았습니다. 저장되는 복사본은 JSON입니다. 나중의 `response()`는 튜플을 목록으로 돌려줍니다.

**예외.** `InvalidIdentifier`입니다. 효과의 겹쳐 쓰기는 `GuardError`입니다. `int`가 아닌 code는 `TypeError`입니다. `NaN`/`Infinity`이거나 너무 큰 응답은 `ValueError`입니다(트랜잭션 안에서 검사하므로 효과까지 롤백됩니다). 효과가 던지는 모든 것은 롤백 후 그대로 나옵니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

def effect(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
    conn.execute("UPDATE accounts SET balance = balance - 5 WHERE id = ?", (7,))
    return 200, {"charged": True}

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    digest = request_digest("POST", "/charges", b'{"order": 7}')
    guard.claim("client-a", "req-1", digest)
    result = guard.apply_pending("client-a", "req-1", digest, effect)
    assert result == (200, {"charged": True})
    guard.close()
```

### `ReplayGuard.reserve_outbound(intent_id: str, fixed: Mapping[str, str]) -> dict`

바깥쪽 멱등성입니다. 받는 쪽이 요청 번호로 중복을 제거하는 요청을 보내기 전에, 비즈니스 `intent_id`에 대한 고정된 `request_id`와 시각을 저장해 두십시오. 같은 의도를 같은 `fixed` 내용으로 다시 부르면 같은 컨텍스트를 돌려받습니다. 그래서 몇 번을 다시 보내도 받는 쪽에는 하나의 요청으로 보입니다. `fixed`는 그대로 저장됩니다. 본문 해시를 넣으십시오. 본문이나 비밀값을 넣지 마십시오. 같은 의도 id를 다른 내용으로 재사용하면 `IntentMismatch`가 납니다. 그때 원래 예약은 살아 남습니다.

**매개변수.** `intent_id` — 비즈니스 의도의 안정적인 식별자입니다. `fixed` — 요청을 서술하는 `str` → `str` 사전입니다(끝점, 본문 해시 등). 예약된 키인 `request_id`나 `ts`를 담으면 안 됩니다.

**반환값.** 모든 `fixed` 항목과 생성된 `"request_id"`(uuid hex), `"ts"`(문자열화한 시각)를 담은 사전입니다. 매 재시도에서 같은 사전입니다.

**예외.** `InvalidIdentifier`입니다. `fixed`가 `str` 키와 `str` 값을 가진 `Mapping`이 아니면 `TypeError`입니다. `fixed`가 `request_id`나 `ts`를 담으면 `ValueError`입니다. 다른 내용으로 재사용하면 `IntentMismatch`입니다. `sqlite3.OperationalError`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    ctx = guard.reserve_outbound("email-batch-77",
                                 {"to": "ops@example.com", "body_sha256": "9f2c" * 16})
    assert guard.reserve_outbound("email-batch-77",
                                  {"to": "ops@example.com", "body_sha256": "9f2c" * 16}) == ctx
    print("sending with request_id", ctx["request_id"])   # a retry gets the same id
    guard.close()
```

### `ReplayGuard.close() -> None`

내부의 SQLite 연결을 닫습니다. 이 뒤에는 모든 메서드가 `sqlite3.ProgrammingError`를 냅니다. 컨텍스트 관리자 형태가 자동으로 부릅니다.

**매개변수.** 없습니다. **반환값.** 없습니다. **예외.** 닫기가 실패하면 `sqlite3.Error`입니다.

```python
import tempfile

from joons_agent_guard import ReplayGuard, request_digest

with tempfile.TemporaryDirectory() as tmp:
    guard = ReplayGuard(tmp + "/guard.db")
    digest = request_digest("POST", "/charges", b'{"order": 7}')
    try:
        guard.claim("client-a", "req-1", digest)
    finally:
        guard.close()                           # connection really closed
```

---

## AsyncResourceLock

```text
AsyncResourceLock(path, *, timeout: float = 10.0)
```

`ResourceLock`의 코루틴(coroutine) 감싸개입니다. 동기 인스턴스 하나를 소유하고 모든 블로킹 호출을 `asyncio.to_thread`로 보냅니다(파이썬 3.9 이상). 경쟁하는 SQLite 쓰기는 작업자 스레드에서 기다립니다. 그래서 이벤트 루프는 계속 움직입니다. 메서드 이름과 의미는 동기 클래스와 같지만 메서드는 코루틴이 됩니다. `hold`는 비동기 컨텍스트 관리자가 됩니다. `aclose()`와 `async with`가 소유한 인스턴스를 닫습니다. 생성자는 동기이며 호출한 스레드에서 스키마를 만듭니다. 진행 중인 호출을 취소하면 작업은 곧바로 `CancelledError`로 끝납니다. 그 사이 SQLite 작업은 스레드에서 끝까지 실행됩니다. 결과는 `status`/`release`로 계속 확인할 수 있습니다(`aio.py` 모듈 문서를 보십시오).

**메서드.** `acquire(resource: str, owner: str, job_id: str) -> bool`, `release(resource: str, owner: str, job_id: str) -> bool`, `status(resource: str) -> joons_agent_guard.lock.LockInfo | None`, `list_locks() -> List[joons_agent_guard.lock.LockInfo]`, `break_lock(resource: str, expected_job_id: str) -> bool`, `hold(resource: str, owner: str, job_id: str)`(비동기 컨텍스트 관리자), `aclose() -> None` — 각각 같은 이름의 동기 메서드를 반영합니다. 같은 예외도 냅니다(`InvalidIdentifier`, 바쁜 `hold`의 `LockHeld`, `sqlite3.OperationalError`).

**예제.**

```python
import asyncio
import tempfile

from joons_agent_guard import AsyncResourceLock

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        async with AsyncResourceLock(tmp + "/guard.db") as lock:
            if await lock.acquire("deploy-api", "worker-1", "job-42"):
                print("deploying")                 # the call runs on a worker thread
                await lock.release("deploy-api", "worker-1", "job-42")

asyncio.run(main())
```

## AsyncCircuitBreaker

```text
AsyncCircuitBreaker(path, *, timeout: float = 10.0)
```

`CircuitBreaker`의 코루틴 감싸개입니다. 어댑터마다 동기 인스턴스 하나를 소유하고 모든 블로킹 호출을 `asyncio.to_thread`로 보냅니다. 메서드 이름과 의미는 코루틴으로 같고, `aclose()`와 `async with`가 수명 주기를 닫습니다. 생성자에 관한 주의사항은 위의 `AsyncResourceLock`과 같습니다.

**메서드.** `begin(actor: str, job_id: str) -> bool`, `finish(actor: str, job_id: str, *, success: bool, reason: str | None = None) -> bool`(4096자보다 긴 `reason`은 저장되기 전에 잘립니다), `snapshot(actor: str) -> joons_agent_guard.circuit.CircuitState`, `reset(actor: str, expected_job_id: str | None = None) -> bool`, `aclose() -> None` — 각각 같은 이름의 동기 메서드를 반영합니다. 같은 예외도 냅니다(`InvalidIdentifier`, `sqlite3.OperationalError`).

**예제.**

```python
import asyncio
import tempfile

from joons_agent_guard import AsyncCircuitBreaker

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        async with AsyncCircuitBreaker(tmp + "/guard.db") as breaker:
            if await breaker.begin("deploy-worker", "job-42"):
                await breaker.finish("deploy-worker", "job-42", success=True)

asyncio.run(main())
```

## AsyncReplayGuard

```text
AsyncReplayGuard(path, *, timeout: float = 15.0)
```

`ReplayGuard`의 코루틴 감싸개입니다. 어댑터마다 동기 인스턴스 하나를 소유하고 모든 블로킹 호출을 `asyncio.to_thread`로 보냅니다. 메서드 이름과 의미는 코루틴으로 같고, `aclose()`와 `async with`가 수명 주기를 닫습니다. `apply_pending` 효과는 작업자 스레드에서 도는 평범한 동기 함수입니다. 그 안에서 asyncio를 쓰면 안 됩니다.

**메서드.** `claim(principal: str, request_id: str, digest: str) -> str`, `complete(principal: str, request_id: str, digest: str, code: int, response: Any) -> bool`, `response(principal: str, request_id: str, digest: str) -> Tuple[int, Any] | None`(해독할 수 없는 저장 응답에는 `CorruptStorage`를 냅니다), `apply_pending(principal: str, request_id: str, digest: str, effect: Callable[[Any], Tuple[int, Any]]) -> Tuple[int, Any] | None`, `reserve_outbound(intent_id: str, fixed: Mapping[str, str]) -> dict`, `aclose() -> None` — 각각 같은 이름의 동기 메서드를 반영합니다. `NaN`/`Infinity`이거나 너무 큰 응답의 `ValueError`와 `max_response_bytes` 상한을 포함해 같은 예외를 냅니다.

**예제.**

```python
import asyncio
import tempfile

from joons_agent_guard import AsyncReplayGuard, request_digest

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        async with AsyncReplayGuard(tmp + "/guard.db") as guard:
            digest = request_digest("POST", "/charges", b'{"order": 7}')
            state = await guard.claim("client-a", "req-1", digest)
            if state == "new":
                await guard.complete("client-a", "req-1", digest, 200, {"charged": True})
            print(state, await guard.response("client-a", "req-1", digest))

asyncio.run(main())
```

## 명령줄 도구(Command line tool)

`python -m joons_agent_guard --db PATH [--json] COMMAND ...`은 같은 데이터베이스 파일 위에서 도는 운영자 도구입니다. 읽기 명령(`locks`, `circuit`, `pending`)은 `SELECT` 문만 실행합니다. 표를 만들지 않고, 요청을 차지하지 않고, 상태를 바꾸지도 않습니다. 구성 요소를 쓴 적이 없는 데이터베이스도 괜찮습니다. 읽기 명령은 데이터베이스를 엄격히 읽기 전용(`mode=ro`와 `PRAGMA query_only`)으로 엽니다. 그래서 디스크의 어떤 것도 만들거나 고치지 않습니다. 그리고 멈춘 쓰기가 남긴 핫 저널(hot journal)은 조용히 롤백되는 대신 오류로 보고됩니다. 두 복구 명령(`break-lock`, `reset-circuit`)은 감사되는 라이브러리 API를 부릅니다. `--yes`가 있을 때만 움직입니다. 없으면 무엇을 할지 출력하고 끝 코드 2로 끝납니다.

**전역 옵션.** `--db PATH`(필수) — 가드의 SQLite 데이터베이스 파일입니다. `--json` — 평문 대신 기계가 읽는 JSON입니다. 두 플래그 모두 명령 앞이나 뒤에서 받습니다.

| 명령 | 인자 | 하는 일 |
| --- | --- | --- |
| `locks` | — | 잡힌 잠금을 나열합니다(자원, owner, job_id, 나이) |
| `circuit` | `ACTOR` | 한 작업자의 스냅숏을 출력합니다(상태, 진행 중 작업, 실패 수, 이유). 그 actor의 회로 기록이 없으면 끝 코드 1입니다 |
| `pending` | `[--older-than SECONDS]` | `pending` 상태의 재생 요청을 읽기 전용으로 나열합니다(principal, request_id, digest 접두어, 나이). `--older-than`을 주면 기록된 claim 시각이 그만큼 오래된 행만 남습니다(claim 시각이 없는 행은 이 필터로는 나열되지 않습니다) |
| `break-lock` | `RESOURCE --job-id ID [--yes]` | `--yes`가 없으면 무엇을 할지 출력합니다(끝 코드 2). `--yes`가 있으면 현재 잠금자의 job_id가 맞을 때만 잠금을 제거합니다(맞는 것이 없으면 끝 코드 1) |
| `reset-circuit` | `ACTOR [--job-id ID] [--yes]` | `--yes`가 없으면 무엇을 할지 출력합니다(끝 코드 2). `--yes`가 있으면 actor를 closed로 리셋합니다. 필요하면 주어진 작업이 아직 진행 중일 때만입니다(맞는 것이 없으면 끝 코드 1) |

나이는 운영자를 위한 정보입니다. 아무것도 자동으로 다시 실행하는 근거가 아닙니다. pending은 여전히 "모른다"는 뜻입니다.

**끝 코드.** `0`은 정상입니다. `1`은 대상 없음 또는 바뀐 것 없음입니다(모르는 actor, 없는 데이터베이스 파일, 맞는 것이 없던 복구 작업). `2`는 확인 필요 또는 잘못된 사용입니다(`--yes` 없는 복구 명령, 모르는 명령, 빠진 인자, 음수 `--older-than`).

**파이썬 안에서.** 같은 파서(parser)는 `joons_agent_guard.cli`의 `cli.main(argv: List[str] | None = None) -> int`로도 쓸 수 있습니다. 프로세스를 끝내는 대신 끝 코드를 돌려줍니다.

```bash
python -m joons_agent_guard --db guard.db locks
python -m joons_agent_guard --db guard.db circuit deploy-worker
python -m joons_agent_guard --db guard.db pending --older-than 3600 --json
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42          # plan only, exit 2
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42 --yes    # apply
python -m joons_agent_guard --db guard.db reset-circuit deploy-worker --yes
```

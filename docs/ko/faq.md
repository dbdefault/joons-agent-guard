# JOONS Agent Guard — 자주 묻는 질문(FAQ)

English original: [docs/faq.md](../faq.md)

`joons_agent_guard` 패키지(오래가는 SQLite 기반의 잠금(lock), 회로 차단기(CircuitBreaker), 중복 실행 방지(ReplayGuard))를 쓰는 분들을 위한 답변 모음입니다. 아래의 모든 예제는 파이썬 3.9 이상에서 그대로 실행됩니다. 표준 라이브러리 밖의 의존성은 없습니다. 각 예제는 현재 디렉터리에 `guard.sqlite`라는 파일을 만들거나 사용합니다.

---

### 1. 잠금은 왜 저절로 만료(expiry)되지 않으며, 작업자가 멈춘 뒤에는 어떻게 복구하나요?

잠금을 쥔 채로 멈춘 작업자는 어디까지 진행됐는지 알 수 없는 작업을 보호하고 있을 수 있습니다. 절반만 끝난 쓰기나 이미 일어난 부수 효과가 그 예입니다. 조용히 잠금을 빼앗으면 데이터가 망가질 수 있습니다. 그래서 이 라이브러리는 잠금을 나이로 제거하지 않습니다. 운영자가 상황을 살펴보고 명시적으로 잠금을 풀 때까지 `acquire`는 계속 `False`를 돌려줍니다. 잠금자는 `status()`나 `list_locks()`로 확인합니다. 기록된 `job_id`가 정말 죽었는지 확인한 뒤 `break_lock(resource, expected_job_id)`를 부릅니다. 이 함수는 사이에 다른 작업이 잠금을 다시 잡았다면 삭제를 거절합니다. 그 뒤의 `acquire`는 성공합니다. 명령줄도 같은 복구를 제공합니다. `python -m joons_agent_guard --db guard.db break-lock RESOURCE --job-id ID --yes`입니다.

```python
from joons_agent_guard import ResourceLock

lock = ResourceLock("guard.sqlite")
print('status:', lock.status("deploy-api"))                 # who holds it, and since when
print('break:', lock.break_lock("deploy-api", "job-42"))   # only after confirming the holder is dead
print('acquire:', lock.acquire("deploy-api", "worker-1", "job-43"))
```

### 2. 재생 요청이 처리 중(pending)인 동안 내 프로세스가 죽으면 어떻게 됩니까?

행은 영원히 `pending` 상태로 남습니다. 그 키에 대해 `claim`은 계속 `pending`을 돌려주고, 나이로 다시 실행하지 않습니다. pending은 결과를 모른다는 뜻이기 때문입니다. 작업이 절반만 일어났을 수 있습니다. 이것이 안전을 위한 거래입니다. 두 번 실행은 없지만, 사람이 복구하기 전까지 그 키는 막혀 있습니다. 순수한 SQLite 쓰기인 효과(effect)는 검토된 복구 경로인 `apply_pending`을 쓰십시오. 이 함수는 효과를 실행하고 응답을 하나의 트랜잭션(transaction)으로 저장합니다. 복구 자체를 재시도하거나 경쟁시켜도 효과는 많아야 한 번 일어납니다. 효과는 SQLite 전용이어야 합니다. 그리고 같은 `ReplayGuard` 인스턴스의 쓰기 메서드를 부르면 안 됩니다. 겹쳐 쓰기는 아무것도 쓰이기 전에 `GuardError`를 냅니다. 저장되는 응답은 JSON입니다. 그래서 `response()`는 JSON 왕복 결과를 돌려줍니다(튜플은 목록이 되고, 문자열이 아닌 사전 키는 문자열이 됩니다). JSON에서 안정적인 타입을 쓰십시오. 파일, 네트워크 호출, 서브프로세스에 대해서는 가드가 아무것도 롤백할 수 없습니다. 실제로 무엇이 있었는지 알아낸 뒤 관찰한 결과를 `complete()`로 기록하십시오.

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")
digest = request_digest("POST", "/charge", b'{"order": 7}')

state = guard.claim("client-a", "req-001", digest)
if state == "new":
    # do the real work here, then record the outcome
    guard.complete("client-a", "req-001", digest, 200, {"charged": True})
elif state == "pending":
    # reviewed crash recovery for a SQLite-only effect: the effect and the
    # stored response commit together, at most once
    guard.apply_pending(
        "client-a", "req-001", digest,
        lambda conn: (200, "recovered"),  # run your SQL through conn here
    )
print(state, guard.response("client-a", "req-001", digest))
```

### 3. DB 파일 하나를 세 구성 요소가 함께 쓸 수 있습니까?

네, 가능합니다. 각 구성 요소는 `CREATE TABLE IF NOT EXISTS`로 자기 표만 만듭니다. `ResourceLock`은 `resource_locks`를, `CircuitBreaker`는 `worker_circuits`를, `ReplayGuard`는 `requests`와 `outbound_requests`를 만듭니다. 그래서 서로 방해하지 않고 한 파일에 함께살 수 있습니다. 대가는 경쟁입니다. 어느 구성 요소의 쓰기든 같은 SQLite 쓰기 잠금을 거칩니다. 느린 `apply_pending` 효과는 잠금과 회로 작업까지 늦춥니다. 편의를 위해서는 파일을 함께 쓰십시오. 무겁고 독립적인 작업 흐름에는 파일을 따로 주십시오.

```python
from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock

path = "guard.sqlite"
with ResourceLock(path), CircuitBreaker(path), ReplayGuard(path):
    pass  # each component just creates its own tables in the same file
```

### 4. 인스턴스 하나를 여러 스레드가 함께 쓸 수 있습니까?

네, 가능합니다. 모든 쓰기 트랜잭션(`BEGIN IMMEDIATE` ... `COMMIT`)과 모든 읽기는 인스턴스 안의 재진입 잠금(re-entrant lock)을 거칩니다. 그래서 스레드들의 문장이 한 트랜잭션 안에서 섞이는 일은 없습니다. 공유 인스턴스는 호출을 한 줄로 세웁니다. 읽기 병렬성을 더 원하면 아래처럼 같은 경로에 대해 스레드마다 인스턴스를 따로 줄 수도 있습니다. 두 패턴 모두 시험 모음이 다룹니다. 프로세스를 넘어서는 직렬화는 SQLite의 파일 잠금이 합니다.

```python
import threading

from joons_agent_guard import ReplayGuard, request_digest

digest = request_digest("POST", "/job", b"{}")

def worker():
    guard = ReplayGuard("guard.sqlite")  # one instance per thread (sharing one also works)
    try:
        print(guard.claim("client-a", f"req-{threading.get_ident():x}", digest))
    finally:
        guard.close()

threads = [threading.Thread(target=worker) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

### 5. 여러 컴퓨터에서 동작합니까?

아니요, 그럴 수 없습니다. SQLite는 로컬 디스크의 데이터베이스입니다. 함께 일하려는 모든 프로세스는 같은 컴퓨터의 같은 파일을 열어야 합니다. 잠금과 회로 자리와 재생 판정은 그곳에만 존재합니다. DB 파일을 네트워크 드라이브나 클라우드 드라이브에 두지 마십시오. 네트워크 파일 시스템 위의 파일 잠금은 믿을 수 없습니다. 보장이 조용히 깨질 수 있습니다. 컴퓨터가 여러 대라면 디스크를 쥔 컴퓨터에서 가드를 실행하십시오. 그리고 자신의 API로 판정을 다른 컴퓨터에 노출하십시오.

### 6. DB는 얼마나 커지며, 오래된 재생 행은 어떻게 안전하게 치웁니까?

DB는 요청량만큼 커집니다. `(principal, request_id)`마다 `requests` 행이 하나 영원히 남습니다. 의도(intent)마다 `outbound_requests` 행이 하나 영원히 남습니다. 각 행은 응답을 JSON 텍스트로 저장합니다. 잠금과 회로 표는 작게 머뭅니다(자원이나 작업자마다 한 행입니다). 오래된 행은 SQL로 직접 치우십시오. 대상은 `state='completed'`인 행뿐입니다. 그리고 재시도가 아직 도착할 수 있는 어떤 시간창보다 오래된 행뿐입니다. 완료된 행을 지우면 같은 키의 반복이 다시 `new`로 차지될 수 있습니다. `pending` 행은 절대 지우지 마십시오. 그 결과는 모르며, 지우면 두 번 실행이 가능해집니다. `requests` 표는 claim 시각(`claimed`)만 기록합니다. 그래서 완료된 행의 나이 규칙은 여전히 직접 정해야 합니다. 날짜를 넣은 `request_id`가 한 예입니다. 응답 JSON 안에 완료 시각을 저장하는 것도 예입니다. 지워진 페이지는 새 행이 재사용합니다. 파일을 실제로 줄이려면 `VACUUM`을 실행하십시오.

```python
import sqlite3

from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")   # ensures the tables exist
digest = request_digest("POST", "/backup", b"")
guard.claim("client-a", "2026-10-31-job", digest)      # a date prefix in request_id
guard.complete("client-a", "2026-10-31-job", digest, 200, {"ok": True})
guard.close()

con = sqlite3.connect("guard.sqlite")
# WARNING: completed rows only, and only older than your retry window.
# Never delete 'pending' rows. Completed rows carry no completion time, so the
# age filter comes from your own convention (here: a date prefix in request_id).
con.execute("DELETE FROM requests WHERE state='completed' AND request_id LIKE '2026-10-%'")
con.commit()
con.execute("VACUUM")  # return freed pages to the OS
con.close()
```

### 7. 대기 시간(timeout)은 얼마로 정해야 합니까?

생성자의 `timeout`은 SQLite의 바쁨 대기 시간(busy timeout)입니다. 기본값은 `ResourceLock`과 `CircuitBreaker`가 10초이고 `ReplayGuard`가 15초입니다. 이 값만큼 다른 프로세스의 쓰기 트랜잭션을 기다린 뒤 `sqlite3.OperationalError: database is locked`를 냅니다. 평범한 작업은 밀리초 안에 끝나는 아주 작은 한 행짜리 트랜잭션입니다. 그래서 기본값도 이미 넉넉합니다. `apply_pending` 효과가 몇 초씩 걸린다면 값을 올리십시오(예: `ReplayGuard(path, timeout=30.0)`). 효과는 쓰기 트랜잭션 안에서 실행되므로 다른 모든 쓰기를 막습니다. 이 timeout은 잠금이나 pending 요청이나 회로 자리의 만료가 아닙니다. 이들은 스스로 시간이 다해 끝나는 일이 없습니다.

### 8. request_id는 어떻게 정하고, digest에는 무엇이 들어갑니까?

`request_id`는 하나의 논리적 작업을 식별해야 합니다. 그리고 그 작업의 모든 재시도에서 같아야 합니다. 보통은 호출자가 주는 멱등(idempotent) 키입니다. 기본 키는 `(principal, request_id)`입니다. 따라서 principal이 다르면 같은 키가 만나는 일은 없습니다. digest는 `request_digest`가 계산하는 값입니다. HTTP 방식과 경로, 본문의 SHA-256, 키워드로 넘긴 추가 문자열 필드를 담은 정규화된 JSON 위의 SHA-256입니다. 작업의 의미를 바꾸는 모든 것을 넣으십시오. `body_sha256`은 예약된 이름입니다. 이 값은 언제나 본문에서 계산되므로, 키워드로 넘기면 `ValueError`가 납니다. 모든 필드 값은 `str`이어야 합니다(아니면 `TypeError`). 같은 `(principal, request_id)`에 다른 digest가 오면 `claim`은 실행하거나 응답을 주는 대신 `conflict`를 돌려줍니다. 이것은 거의 언제나 클라이언트가 같은 키를 다른 일에 재사용했다는 뜻입니다. 평범한 문자열 `'new'` / `'pending'` / `'completed'` / `'conflict'`를 비교하는 것은 `joons_agent_guard.replay`가 내보내는 상수를 비교하는 것과 같습니다.

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")
body = b'{"order": 7}'
idempotency_key = "k-9"
digest = request_digest("POST", "/charges", body, tenant="t1")
print(guard.claim("client-a", idempotency_key, digest))
```

### 9. asyncio와 함께 쓸 수 있습니까?

네, 가능합니다. 패키지에 들어 있는 asyncio 어댑터(adapter)인 `AsyncResourceLock`, `AsyncCircuitBreaker`, `AsyncReplayGuard`를 쓰십시오(파이썬 3.9 이상). 각 어댑터는 동기 인스턴스 하나를 소유하고 모든 호출을 `asyncio.to_thread`로 보냅니다. 경쟁하는 SQLite 쓰기는 작업자 스레드에서 기다립니다. 그래서 이벤트 루프(event loop)는 계속 움직입니다. 메서드는 같은 이름과 의미로 코루틴(coroutine)이 됩니다. `hold`는 비동기 컨텍스트 관리자가 됩니다. `async with`와 `aclose()`가 인스턴스를 닫습니다. `apply_pending` 효과는 그 작업자 스레드에서 도는 평범한 동기 함수입니다.

```python
import asyncio

from joons_agent_guard import AsyncReplayGuard, request_digest


async def handle(principal, request_id, digest):
    async with AsyncReplayGuard("guard.sqlite") as guard:
        state = await guard.claim(principal, request_id, digest)
        if state == "new":
            await guard.complete(principal, request_id, digest, 200, {"done": True})
        return state, await guard.response(principal, request_id, digest)


print(asyncio.run(handle("client-a", "req-async-1", request_digest("POST", "/async", b"{}"))))
```

### 10. 어떤 파이썬 버전과 플랫폼이 지원됩니까?

이 패키지는 파이썬 3.9 이상을 필요로 합니다. 표준 라이브러리 밖의 의존성은 없습니다. 그래서 CPython이 `sqlite3` 모듈을 제공하는 곳이면 어디서든 실행됩니다. Windows, macOS, Linux이고, 컴파일러도 네이티브 의존성도 없습니다. 설치할 서버도 없고 띄울 데몬도 없습니다. 구성 요소는 하나의 SQLite 파일 위에서 도는 평범한 객체입니다. DB 파일 자체는 플랫폼을 넘어 옮길 수 있습니다. 옮기거나 복사해도 됩니다. 다만 두 컴퓨터가 동시에 쓰게 하지는 마십시오.

### 11. 평범한 파일 잠금과는 어떻게 다릅니까?

운영체제의 파일 잠금은 약속일 뿐입니다. 살아 있는 프로세스 핸들에 묶여 있습니다. 프로세스가 끝나면 사라집니다. 편리하지만 누가 자원을 쥐고 있었는지 알려주지 않습니다. 그 작업이 끝났는지도 알려주지 않습니다. 다른 것도 저장하지 않습니다. 이 라이브러리의 잠금은 owner와 job id와 획득 시각을 기록하는 영속적인 행입니다. 일부러 멈춤을 넘겨 남기므로, 복구 결정은 `break_lock`으로 사람에게 남습니다. 또한 재생 표와 회로 표 옆에 삽니다. 그래서 하나의 DB가 배타와 감사와 많아야 한 번 실행을 함께 줍니다.

### 12. 재시도 라이브러리와는 어떻게 다릅니까?

재시도 도우미는 실패 뒤에 함수를 다시 실행합니다. 이것은 최소 한 번(at-least-once) 실행이며, 멱등한 작업에만 안전합니다. `ReplayGuard`는 문제의 나머지 절반을 풉니다. 재시도를 계획하거나 조절하지 않습니다. 대신 같은 요청이 몇 번 도착하더라도 처리기는 많아야 한 번 실행됨을 보장합니다. 이후 도착은 저장된 응답을 받습니다. 둘은 잘 맞물립니다. 전달은 마음껏 재시도하십시오. 실행의 중복 제거는 가드에 맡기십시오.

### 13. 메모리 안의 회로 차단기와는 어떻게 다릅니까?

메모리 안의 차단기는 한 프로세스 안에서 삽니다. 상태는 재시작에 사라집니다. 열림 상태는 형제 프로세스에게 보이지 않습니다. 그래서 다시 시작된 작업자가 다른 작업자가 방금 기록한 실패를 곧바로 반복할 수 있습니다. 이 차단기는 작업자별 상태를 SQLite에 저장합니다. `closed`/`open`/`half_open`과 연속 실패 수와 마지막 실패 이유가 그 상태입니다. 같은 파일을 쓰는 모든 프로세스가 함께 봅니다. 이 차단기의 특징은 작업자당 실행 자리 하나입니다. 시작만 하고 끝내지 못한 작업은 자리를 쥡니다. `finish()`가 결과를 기록하거나 운영자가 `reset()`을 부를 때까지입니다. 나이로 치워지는 것은 없습니다.

### 14. claim이 'pending'이나 'conflict'를 돌려줄 때 처리기는 무엇을 해야 합니까?

`pending`은 다른 시도가 진행 중이거나 죽었을 수 있다는 뜻입니다. 결과를 모릅니다. 안전한 답은 "나중에 다시 시도" 스타일의 응답입니다. HTTP 서비스라면 409나 503입니다. 작업을 다시 실행하는 것은 절대 아닙니다. `completed`는 작업이 끝났다는 뜻입니다. `response()`로 저장된 응답을 그대로 돌려주십시오. `conflict`는 같은 `(principal, request_id)`에 다른 digest가 들어왔다는 뜻입니다. 클라이언트 오류로 취급하십시오. 아무것도 실행하지 마십시오.

### 15. reserve_outbound는 언제 필요합니까?

바깥쪽 멱등성을 위해 씁니다. 받는 쪽이 요청 번호로 중복을 제거한다면, 보내기 전에 그 비즈니스 의도에 대한 고정된 `request_id`와 시각을 저장해 두십시오. 재시도 때는 같은 `intent_id`와 같은 `fixed` 내용으로 `reserve_outbound`를 다시 부릅니다. 그러면 같은 컨텍스트를 돌려받습니다. 몇 번을 보내도 받는 쪽에는 하나의 요청으로 보입니다. 같은 의도에 다른 내용을 쓰면 조용히 다른 것을 보내는 대신 `IntentMismatch`가 납니다. `fixed`에는 해시를 저장하십시오. 비밀값이나 전체 본문은 저장하지 마십시오.

```python
from joons_agent_guard import ReplayGuard

guard = ReplayGuard("guard.sqlite")
context = guard.reserve_outbound(
    "email-batch-77",
    {"to": "ops@example.com", "body_sha256": "9f2c" * 16},
)
# use context["request_id"] in the outgoing request; on any retry, reserve the
# same intent_id again and you get the identical context back
print(context)
```

### 16. 명령줄에서 상태를 들여다보고 복구하려면 어떻게 합니까?

패키지에 들어 있는 운영자 도구를 쓰십시오. `python -m joons_agent_guard --db guard.sqlite <command>`입니다. 이 도구는 라이브러리가 쓰는 같은 SQLite 파일을 읽습니다. 따라서 디스크를 쥔 컴퓨터에서 실행하십시오. 읽기 전용 명령은 아무것도 바꾸지 않습니다. `locks`는 잡힌 잠금을 나열합니다(자원, owner, job_id, 나이). `circuit ACTOR`는 한 작업자의 스냅숏(snapshot)을 보여 줍니다. `pending`은 `pending` 상태의 재생 요청을 claim 이후의 나이와 함께 나열합니다. `pending --older-than SECONDS`는 그만큼 오래 전에 claim된 행만 남깁니다. 나이는 운영자를 위한 힌트일 뿐입니다. 아무것도 자동으로 다시 실행하는 근거가 아닙니다. 두 복구 명령은 감사된 라이브러리 호출을 그대로 반영합니다. 확인 없이는 절대 움직이지 않습니다. `break-lock RESOURCE --job-id ID`와 `reset-circuit ACTOR [--job-id ID]`는 무엇을 할지 출력하고 끝 코드 2로 끝납니다. `--yes`를 붙여 다시 실행해야 적용됩니다. `--json`을 붙이면 기계가 읽는 출력이 됩니다. 끝 코드는 다음과 같습니다. 0은 정상입니다. 1은 대상 없음 또는 바뀐 것 없음입니다. 2는 확인 필요 또는 잘못된 사용입니다.

```console
python -m joons_agent_guard --db guard.sqlite locks
python -m joons_agent_guard --db guard.sqlite circuit worker-1
python -m joons_agent_guard --db guard.sqlite pending --older-than 3600
python -m joons_agent_guard --db guard.sqlite break-lock deploy-api --job-id job-42         # plan only, exit code 2
python -m joons_agent_guard --db guard.sqlite break-lock deploy-api --job-id job-42 --yes    # really break it
python -m joons_agent_guard --db guard.sqlite reset-circuit worker-1 --yes                   # operator reset
```

### 17. 비동기 호출을 취소하면 어떻게 됩니까?

취소는 즉각적이지만 중단은 아닙니다. asyncio는 작업자 스레드에서 이미 실행 중인 호출을 멈출 수 없습니다. 따라서 작업은 `CancelledError`로 곧바로 끝나는 동안, SQLite 작업은 그 스레드에서 끝까지 실행됩니다. 커밋은 비동기적으로 도착합니다. 취소 직후에 확인한 상태는 아직 호출 전 값을 보여 줄 수 있습니다. 그러므로 호출이 정리된 뒤에 다시 확인하십시오. 취소된 `claim`은 요청을 `pending`으로 커밋해 두었습니다. 같은 `(principal, request_id, digest)`로 다시 claim하십시오(`pending`을 받습니다. 절대 새 request_id를 만들지 마십시오). 일을 실행하고 같은 digest로 `complete`하십시오. 취소된 `apply_pending`은 두 갈래 모두 복구됩니다. 커밋되었다면 효과는 정확히 한 번 실행되었고 `response()`가 저장된 결과를 줍니다. 완전히 롤백되었다면 행은 `pending`으로 남고 다시 적용하면 효과를 다시 실행합니다. 취소 뒤에 작업자 스레드가 낸 오류는 버려집니다. 결과를 알아내려면 상태를 확인하십시오. 취소된 `hold` 진입은 자신의 `acquire`가 끝나기를 기다리고, 잠금을 가져갔다면 놓습니다(본문은 실행되지 않습니다). 취소된 `hold` 종료는 언제나 놓습니다. `aclose()`는 이미 진행 중이던 호출이 끝나기를 기다린 뒤 닫습니다. `aclose()`가 시작된 뒤에 시작된 호출은 `sqlite3.ProgrammingError`를 냅니다.

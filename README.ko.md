# JOONS Agent Guard

AI 에이전트와 자동화 작업을 위한, 꺼져도 사라지지 않는 안전장치 모음입니다.

> [JOONS 생태계](https://github.com/dbdefault/joons-ecosystem)(MCP 기반 AI 인프라 생태계)의 일부입니다.

English: [README.md](README.md)

[![CI](https://github.com/dbdefault/joons-agent-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/dbdefault/joons-agent-guard/actions/workflows/ci.yml)

## 한눈에 보기

- **무엇**: 하나의 SQLite 파일로 뒷받침되는, 영속적인 잠금과 작업자별 회로 차단기, 그리고 많아야 한 번 요청 처리입니다. 표준 라이브러리만 쓰며 파이썬 3.9 이상입니다.
- **대상**: 한 컴퓨터에서, 하나의 공유 데이터베이스 파일 위에서 다시 시도하고 멈추고 동시에 도는 에이전트와 스크립트입니다.
- **대상이 아님**: 컴퓨터를 넘나드는 조율, 높은 처리량의 파이프라인(쓰기는 하나입니다. 개발용 디스크에서는 초당 대략 백 개의 잠금 쌍 규모입니다. `bench/README.md`를 보십시오), 또는 상태가 나이로 만료되기를 필요로 하는 어떤 일도 아닙니다.
- **방법**: 소스에서 설치하고, 모든 프로세스가 같은 데이터베이스 파일을 가리키게 합니다. `docs/recipes.md`에서 레시피를 하나 복사하고, 끼어 있는 상태는 `python -m joons_agent_guard --db guard.db locks`로 들여다봅니다.
- **신뢰**: 잠금은 약속일 뿐입니다. 호출자는 인증되지 않습니다. 그리고 아무것도 만료되지 않습니다. 의지하기 전에 `docs/guarantees.md`를 읽으십시오.

## 왜 필요한가요

에이전트와 스크립트는 다시 시도하고, 도중에 멈추고, 여러 개가 동시에 돌아갑니다. 이때 여러 프로세스가 함께 믿을 수 있는 기록이 없으면 흔히 세 가지 사고가 납니다.

- **같은 일이 두 번 실행됩니다.** 다시 보낸 요청이 한 번 더 처리되어 주문, 메일, 배포가 두 번 일어납니다.
- **두 작업자가 한 파일을 동시에 고칩니다.** 자원의 주인이 정해지지 않아 서로의 결과를 덮어씁니다.
- **고장 난 서비스를 계속 두드립니다.** 실패한 작업자가 다시 시작되자마자, 누가 원인을 살펴보기도 전에 같은 일을 또 시도합니다.

JOONS Agent Guard는 이 문제를 막는 작은 파이썬 라이브러리입니다. 표준 라이브러리와 SQLite만 쓰고, 따로 설치할 의존성이 없습니다. 세 가지 도구를 제공합니다.

- **잠금(ResourceLock)**: 이름 붙인 자원을 한 번에 한 작업만 쓰게 합니다. 같은 DB 파일을 여는 모든 프로세스가 이 잠금을 함께 봅니다.
- **회로 차단기(CircuitBreaker)**: 작업자(actor)마다 실행 자리를 하나만 둡니다. 실패하면 닫힘(closed), 열림(open), 반열림(half-open) 순서로 상태를 바꾸며 회복을 시도합니다.
- **중복 실행 방지(ReplayGuard)**: 다시 보낸 요청을 많아야 한 번만 처리합니다. 밖으로 보내는 요청에는 바뀌지 않는 요청 번호와 시각을 붙여 줍니다.

## 설치

파이썬 3.9 이상이면 됩니다(Windows, macOS, Linux). 표준 라이브러리 밖의 의존성은 없습니다. 이 패키지는 아직 PyPI에 없습니다.

```bash
pip install git+https://github.com/dbdefault/joons-agent-guard.git
```

소스에서 바로 설치할 수도 있습니다.

```bash
git clone https://github.com/dbdefault/joons-agent-guard.git
cd joons-agent-guard
pip install .
```

## 30초 예제

세 도구는 SQLite DB 파일 하나를 함께 써도 됩니다. 도구마다 다른 표를 쓰기 때문입니다. 아래의 예제는 현재 디렉터리에 `guard.db`를 만들고 그 자리에 남겨 둡니다. 그 파일이 상태 전부이므로, 처음부터 다시 하려면 지우면 됩니다(예제를 두 번 실행하면 멱등(idempotent)한 경로가 보입니다).

자원(resource)은 프로세스들이 서로 정한 이름일 뿐입니다. 라이브러리는 그 파일 자체를 열거나 잠그지 않습니다. 파일 작업은 블록 안에서 직접 하십시오.

### 자원 잠그기

```python
from joons_agent_guard import LockHeld, ResourceLock

lock = ResourceLock("guard.db")

# Exclusive access to a named resource, shared by every process
# that opens the same database file.
try:
    with lock.hold("reports/daily.csv", owner="worker-1", job_id="job-42"):
        print("locked: no other worker can hold 'reports/daily.csv' now")
except LockHeld as exc:
    print(f"someone else holds the lock: {exc}")

# After the with-block the lock is released again.
print("status after release:", lock.status("reports/daily.csv"))

# Locks never expire: if a holder crashed, an operator checks the
# situation and then removes the lock explicitly.
# lock.break_lock("reports/daily.csv", expected_job_id="job-42")

lock.close()
```

### 작업자마다 실행 자리 하나

```python
from joons_agent_guard import CircuitBreaker

breaker = CircuitBreaker("guard.db")
actor = "deploy-worker"

if breaker.begin(actor, job_id="job-42"):
    try:
        print("running job-42: the only job this actor may run right now")
    except Exception as exc:
        breaker.finish(actor, "job-42", success=False, reason=str(exc))
    else:
        breaker.finish(actor, "job-42", success=True)
else:
    state = breaker.snapshot(actor)
    print(f"{state.pending_job} is still in flight; state={state.state}")

print("after the job:", breaker.snapshot(actor))
breaker.close()
```

### 요청을 많아야 한 번만 처리하기

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.db")


def do_work(method, path, body):
    return {"echo": body.decode()}  # your real side effect goes here


def handle_request(principal, request_id, method, path, body):
    """Idempotent request handling: a retry gets the stored response."""
    digest = request_digest(method, path, body)
    outcome = guard.claim(principal, request_id, digest)

    if outcome == "completed":                     # a retry: replay the stored response
        return guard.response(principal, request_id, digest)
    if outcome == "pending":                       # a previous attempt is still in flight
        return (503, {"error": "previous attempt still in flight"})
    if outcome == "conflict":                      # same request_id, different payload
        return (409, {"error": "request_id reused with a different payload"})

    result = do_work(method, path, body)           # outcome == "new": run it once
    guard.complete(principal, request_id, digest, 200, result)
    return (200, result)


print(handle_request("client-a", "req-1", "POST", "/greet", b"hello"))
print(handle_request("client-a", "req-1", "POST", "/greet", b"hello"))  # replayed
guard.close()
```

## 보장하는 것과 보장하지 않는 것

이 라이브러리는 일부러 보수적으로 만들었습니다. 보장하는 것과 일부러 보장하지 않는 것은 다음과 같습니다.

- **잠금은 저절로 풀리지 않습니다.** 프로세스가 다시 시작되거나 멈춰도 잠금은 남습니다. 잠금을 쥔 작업이 어디까지 진행됐는지 모르기 때문입니다. `release()`는 정확히 같은 `(owner, job_id)`만 풀 수 있습니다. 쥔 작업이 죽은 것을 운영자가 확인했다면 `break_lock(resource, expected_job_id)`로 명시적으로 풉니다.
- **처리 중(pending)인 요청은 자동으로 다시 실행하지 않습니다.** 시간이 지나도 풀리지 않습니다. pending은 "결과를 모른다"는 뜻입니다. 이전 호출이 도중에 죽었을 수 있으므로, 다시 실행할지는 사용하는 쪽이 정합니다.
- **정확히 한 번 실행을 보장하는 것은 `apply_pending`뿐입니다.** 이 함수는 SQLite 안에서만 일어나는 작업과 응답 저장을 한 트랜잭션으로 묶습니다. 파일, 네트워크, 다른 프로그램 실행 같은 작업은 `complete()`와 원자적으로 묶을 수 없습니다. 그 사이에 멈추면 요청은 pending으로 남고, 직접 맞춰 봐야 합니다.
- **`reserve_outbound`는 보내기 전에 불러야 합니다.** 의도(intent)마다 바뀌지 않는 `request_id`와 시각을 저장해 두므로, 다시 보내도 받는 쪽에는 같은 요청 하나로 보입니다. 같은 의도에 다른 내용을 넣으면 `IntentMismatch`가 납니다. `fixed`는 그대로 저장되므로 비밀값이나 본문을 넣지 말고, 본문 대신 해시를 넣습니다.
- **호출한 쪽이 누구인지 확인하지 않습니다.** `principal`, `owner`, `actor` 값은 믿을 수 있는 통신 경로(mTLS 신원, 검증된 서명 등)에서 받아야 합니다. principal이 다르면 이름 공간이 나뉠 뿐이며, 이것은 분리이지 보안이 아닙니다.
- **내구성은 SQLite 파일 하나에 달려 있습니다.** 그 파일만큼만 안전하므로 다른 DB처럼 백업합니다.

## API 한눈에 보기

| 도구 | 하는 일 | 주요 함수 |
| --- | --- | --- |
| `ResourceLock(path)` | 프로세스 사이에 공유되는 자원 잠금 | `hold()`, `acquire()`, `release()`, `status()`, `list_locks()`, `break_lock()` |
| `CircuitBreaker(path)` | 작업자마다 실행 자리 하나, 닫힘·열림·반열림 회복 | `begin()`, `finish()`, `snapshot()`, `reset()` |
| `ReplayGuard(path)` | 요청을 많아야 한 번 처리, 보내는 요청의 멱등 키 | `claim()`, `complete()`, `response()`, `apply_pending()`, `reserve_outbound()` |
| `request_digest(...)` | 요청(방식, 경로, 본문, 추가 항목)의 SHA-256 지문 | `ReplayGuard`의 `digest` 인자로 씁니다 |
| 예외 | 모두 `GuardError`를 물려받습니다 | `LockHeld`, `InvalidIdentifier`(`ValueError`이기도 함), `IntentMismatch`(`ValueError`이기도 함) |

모든 생성자는 `timeout`(초)을 받습니다. DB가 다른 쓰기로 바쁠 때 얼마나 기다릴지 정하는 값입니다.

## 플랫폼 안내

- 모든 상태는 SQLite DB 파일에 있습니다. 세 도구가 파일 하나를 함께 써도 되고, 따로 써도 됩니다.
- DB 파일은 **로컬 디스크**에 둡니다. 네트워크 공유 폴더(NFS, SMB)에서는 SQLite의 파일 잠금을 믿을 수 없고, 이 라이브러리는 그 잠금에 기댑니다.
- 도구마다 연결 하나를 자동 커밋 방식으로 열고, 모든 쓰기를 `BEGIN IMMEDIATE` 트랜잭션으로 감쌉니다. 그래서 같은 PC의 여러 프로세스가 한 DB 파일을 안전하게 함께 씁니다.
- 인스턴스 하나를 여러 스레드가 함께 써도 안전합니다. 트랜잭션, 읽기, `close()`가 모두 내부 재진입 잠금을 거치기 때문입니다. 대신 호출이 한 줄로 서므로, 읽기를 동시에 많이 하려면 스레드마다 인스턴스를 따로 만듭니다.

## 명령줄 도구

코드를 쓰지 않고도 DB 상태를 보고 복구할 수 있습니다.

```bash
python -m joons_agent_guard --db guard.db locks
python -m joons_agent_guard --db guard.db pending --older-than 3600
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42          # plan only, exit code 2
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42 --yes     # really break it
```

`--json`을 붙이면 기계가 읽기 좋은 형식으로 나옵니다. 복구 명령(`break-lock`, `reset-circuit`)은 `--yes`가 없으면 아무것도 바꾸지 않습니다. 끝 코드는 0이 정상, 1이 대상 없음 또는 바뀐 것 없음, 2가 확인 필요 또는 잘못된 사용입니다. 자세한 내용은 docs/api.md에 있습니다.

## 문서

- [docs/guarantees.md](docs/guarantees.md): 보장 범위, 장애 상황과 복구 방법
- [docs/recipes.md](docs/recipes.md): 예약 작업, 에이전트 무리, 멱등 웹훅, 안전한 재시도
- [docs/faq.md](docs/faq.md): 자주 묻는 질문
- [docs/api.md](docs/api.md): API와 명령줄 도구 전체 설명
- [docs/architecture.md](docs/architecture.md): 표 구조, 트랜잭션 방식, 상태 그림
- [docs/mutation-testing.md](docs/mutation-testing.md): 일부러 심은 결함을 시험이 잡아내는지 확인한 기록

Korean translations: docs/ko/guarantees.md, docs/ko/recipes.md, docs/ko/faq.md, docs/ko/api.md, docs/ko/architecture.md

(문서는 영어로 되어 있습니다.)

## 개발

```bash
git clone https://github.com/dbdefault/joons-agent-guard.git
cd joons-agent-guard
pip install -e .[test]
python -m pytest
```

## 라이선스

Apache-2.0

---

JOONS 오픈소스 제품군의 하나입니다.

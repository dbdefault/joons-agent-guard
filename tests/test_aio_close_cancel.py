"""aclose() must not close while a cancelled call is still running on its thread."""
import asyncio
import sqlite3
import time

from joons_agent_guard import AsyncResourceLock


def test_aclose_waits_for_cancelled_in_flight_call(tmp_path):
    path = tmp_path / "g.db"

    async def main():
        lock = AsyncResourceLock(path, timeout=5.0)
        blocker = sqlite3.connect(str(path), isolation_level=None)
        blocker.execute("BEGIN EXCLUSIVE")       # makes acquire wait on the thread
        task = asyncio.ensure_future(lock.acquire("r", "o", "j"))
        await asyncio.sleep(0.2)
        task.cancel()                            # caller gives up, thread keeps waiting
        closer = asyncio.ensure_future(lock.aclose())
        await asyncio.sleep(0.3)
        assert not closer.done()                 # aclose is waiting for the thread
        blocker.execute("COMMIT")
        blocker.close()
        await asyncio.wait_for(closer, 10)
        return task

    started = time.monotonic()
    task = asyncio.run(main())
    assert task.cancelled()
    assert time.monotonic() - started < 10
    # the acquire finished on its thread and committed: state is consistent
    from joons_agent_guard import ResourceLock
    check = ResourceLock(path)
    assert check.status("r") is not None
    check.close()

"""JOONS Agent Guard: durable safety primitives for AI agents and automation.

* ResourceLock   - exclusive locks on named resources (no silent expiry)
* CircuitBreaker - per-actor run slot with failure/recovery states
* ReplayGuard    - at-most-once handling of retried requests
"""
from .aio import AsyncCircuitBreaker, AsyncReplayGuard, AsyncResourceLock
from .circuit import CircuitBreaker, CircuitState
from .errors import CorruptStorage, GuardError, IntentMismatch, InvalidIdentifier, LockHeld
from .lock import LockInfo, ResourceLock
from .replay import ReplayGuard, request_digest

__version__ = "0.1.0"
__all__ = [
    "AsyncCircuitBreaker", "AsyncReplayGuard", "AsyncResourceLock",
    "CircuitBreaker", "CircuitState", "CorruptStorage", "GuardError", "IntentMismatch", "InvalidIdentifier",
    "LockHeld", "LockInfo", "ReplayGuard", "ResourceLock", "request_digest", "__version__",
]

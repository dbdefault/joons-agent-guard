"""Exception types shared by all JOONS Agent Guard components."""


class GuardError(Exception):
    """Base class for every error raised by joons_agent_guard."""


class InvalidIdentifier(GuardError, ValueError):
    """An identifier (resource, owner, job id, request id, ...) is empty or not a string."""


class LockHeld(GuardError):
    """The resource is already locked by another holder."""

    def __init__(self, resource, holder=None):
        self.resource = resource
        self.holder = holder
        super().__init__(f"resource {resource!r} is already locked" + (f" by {holder.owner!r}/{holder.job_id!r}" if holder else ""))


class IntentMismatch(GuardError, ValueError):
    """An outbound intent id was reused with different fixed content."""


class CorruptStorage(GuardError):
    """A stored value could not be decoded (the database file was corrupted or tampered with)."""


#: Upper bound for every identifier (resource, owner, job id, actor, principal,
#: request id, digest, intent id). Keeps a buggy or hostile caller from growing
#: the shared database without limit.
MAX_IDENTIFIER_CHARS = 1024


def require_ids(**values):
    """Raise InvalidIdentifier unless every value is a non-empty str of at most MAX_IDENTIFIER_CHARS."""
    for name, value in values.items():
        if not isinstance(value, str) or not value:
            raise InvalidIdentifier(f"{name} must be a non-empty string")
        if len(value) > MAX_IDENTIFIER_CHARS:
            raise InvalidIdentifier(f"{name} must be at most {MAX_IDENTIFIER_CHARS} characters")

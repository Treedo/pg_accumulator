"""Exception classes for sqlalchemy-accumulator."""

from __future__ import annotations

import re


class AccumulatorError(Exception):
    """Base exception for all pg_accumulator errors."""


class RegisterNotFoundError(AccumulatorError):
    """Register does not exist in the database."""

    def __init__(self, register_name: str) -> None:
        self.register_name = register_name
        super().__init__(f'Register "{register_name}" not found')


class RecorderNotFoundError(AccumulatorError):
    """Recorder value not found in movements table."""

    def __init__(self, recorder: str) -> None:
        self.recorder = recorder
        super().__init__(f'Recorder "{recorder}" not found')


class RegisterExistsError(AccumulatorError):
    """Attempt to create a register that already exists."""

    def __init__(self, register_name: str) -> None:
        self.register_name = register_name
        super().__init__(f'Register "{register_name}" already exists')


class ValidationError(AccumulatorError):
    """Input validation failed."""

    def __init__(self, message: str, field: str | None = None) -> None:
        self.field = field
        super().__init__(message)


# ── PostgreSQL error mapping ─────────────────────────────────────────

_REGISTER_NOT_FOUND_RE = re.compile(
    r'[Rr]egister\s+"?([^"]+)"?\s+(does not exist|not found)', re.IGNORECASE
)
_REGISTER_EXISTS_RE = re.compile(
    r'[Rr]egister\s+"?([^"]+)"?\s+already exists', re.IGNORECASE
)
_RECORDER_NOT_FOUND_RE = re.compile(
    r'[Rr]ecorder\s+"?([^"]+)"?\s+not found', re.IGNORECASE
)


def map_pg_error(err: BaseException) -> None:
    """Map a PostgreSQL exception to a typed accumulator error and raise it.

    If the error does not match any known pattern, it is re-raised as-is.
    """
    msg = str(err)

    m = _REGISTER_NOT_FOUND_RE.search(msg)
    if m:
        raise RegisterNotFoundError(m.group(1)) from err

    m = _REGISTER_EXISTS_RE.search(msg)
    if m:
        raise RegisterExistsError(m.group(1)) from err

    m = _RECORDER_NOT_FOUND_RE.search(msg)
    if m:
        raise RecorderNotFoundError(m.group(1)) from err

    raise err

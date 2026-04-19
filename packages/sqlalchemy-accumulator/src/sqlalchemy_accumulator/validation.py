"""Input validation helpers."""

from __future__ import annotations

import re
from datetime import date, datetime

from .errors import ValidationError
from .types import Register

_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def validate_register_name(name: str) -> None:
    """Validate that *name* is a safe SQL identifier for a register."""
    if not _NAME_RE.match(name):
        raise ValidationError(
            f'Invalid register name "{name}". '
            "Must match [a-z_][a-z0-9_]{{0,62}}",
            field="name",
        )


def validate_identifier(value: str, label: str = "identifier") -> None:
    """Validate a generic SQL identifier (schema, column name)."""
    if not _NAME_RE.match(value):
        raise ValidationError(f'Invalid {label}: "{value}"', field=label)


def validate_dimensions(
    data: dict[str, object],
    dimensions: dict[str, str],
) -> None:
    """Ensure all required dimension keys are present in *data*."""
    for key in dimensions:
        if key not in data or data[key] is None:
            raise ValidationError(f'Missing required dimension "{key}"', field=key)


def validate_movement(data: dict[str, object], register: Register) -> None:
    """Validate a single movement dict before posting."""
    if not data.get("recorder"):
        raise ValidationError('Movement must have a "recorder" field', field="recorder")
    if not data.get("period"):
        raise ValidationError('Movement must have a "period" field', field="period")
    validate_dimensions(data, register._def.dimensions)


def to_timestamp(value: str | date | datetime) -> str:
    """Convert a date/datetime to an ISO-format string suitable for PostgreSQL."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def sql_identifier(name: str) -> str:
    """Quote *name* as a safe SQL identifier. Raises on invalid input."""
    if not _NAME_RE.match(name):
        raise ValidationError(f'Invalid SQL identifier: "{name}"')
    return f'"{name}"'

"""Type definitions for sqlalchemy-accumulator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, TypedDict, Union
from uuid import UUID


# ── PostgreSQL type literals ──────────────────────────────────────────

PgColumnType = Literal[
    "int", "integer", "bigint",
    "numeric", "decimal",
    "text", "varchar",
    "boolean",
    "date", "timestamptz", "timestamp",
    "uuid",
]

RegisterKind = Literal["balance", "turnover"]
TotalsPeriod = Literal["day", "month", "year"]
PartitionBy = Literal["day", "month", "quarter", "year"]
RecorderType = Literal["text", "int", "bigint", "uuid"]


# ── PG → Python type mapping (runtime) ───────────────────────────────

PG_TO_PYTHON: dict[str, type] = {
    "int": int,
    "integer": int,
    "bigint": int,
    "numeric": Decimal,
    "decimal": Decimal,
    "text": str,
    "varchar": str,
    "boolean": bool,
    "date": date,
    "timestamptz": datetime,
    "timestamp": datetime,
    "uuid": UUID,
}


# ── Register definition ──────────────────────────────────────────────

@dataclass(frozen=True)
class RegisterDefinition:
    """Immutable definition of an accumulation register."""

    name: str
    kind: RegisterKind
    dimensions: dict[str, str]
    resources: dict[str, str]
    totals_period: TotalsPeriod = "day"
    partition_by: PartitionBy = "month"
    high_write: bool = False
    recorder_type: RecorderType = "text"


@dataclass(frozen=True)
class Register:
    """A typed register handle used with AccumulatorClient."""

    _def: RegisterDefinition


# ── Movement / query types ────────────────────────────────────────────

class MovementInput(TypedDict, total=False):
    """Movement data for posting. recorder and period are required."""

    recorder: str  # type: ignore[assignment]
    period: Union[str, date, datetime]  # type: ignore[assignment]


# We use plain dicts at runtime; TypedDict is for documentation only.
# Actual movement dicts include dimension + resource keys dynamically.

@dataclass
class BalanceOptions:
    """Options for balance queries."""

    at_date: str | date | datetime | None = None


@dataclass
class TurnoverOptions:
    """Options for turnover queries."""

    date_from: str | date | datetime | None = None
    date_to: str | date | datetime | None = None
    group_by: list[str] | None = None


@dataclass
class MovementsOptions:
    """Options for movements queries."""

    recorder: str | None = None
    date_from: str | date | datetime | None = None
    date_to: str | date | datetime | None = None
    limit: int | None = None
    offset: int | None = None


@dataclass
class AlterRegisterOptions:
    """Options for altering a register."""

    add_dimensions: dict[str, str] | None = None
    add_resources: dict[str, str] | None = None
    high_write: bool | None = None


@dataclass
class RegisterInfo:
    """Detailed register information returned by register_info()."""

    name: str
    kind: RegisterKind
    dimensions: dict[str, str]
    resources: dict[str, str]
    totals_period: TotalsPeriod
    partition_by: PartitionBy
    high_write: bool
    recorder_type: str
    created_at: str
    movements_count: int
    tables: dict[str, str] = field(default_factory=dict)
    partitions: list[Any] | None = None


@dataclass
class RegisterListRow:
    """Summary row from register_list()."""

    name: str
    kind: RegisterKind
    dimensions: int
    resources: int
    movements_count: int
    created_at: str


@dataclass
class AccumulatorConfig:
    """Client configuration."""

    schema: str = "accum"

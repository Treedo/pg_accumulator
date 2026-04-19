"""RegisterHandle — bound register operations."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Connection

from .operations.post import post as _post, unpost as _unpost, repost as _repost
from .operations.read import (
    balance as _balance,
    turnover as _turnover,
    movements as _movements,
)
from .types import Register, BalanceOptions, TurnoverOptions, MovementsOptions


class RegisterHandle:
    """A register handle bound to a specific register for chained operations.

    Provides type-safe ``post``, ``unpost``, ``repost``, ``balance``,
    ``turnover``, and ``movements`` methods.
    """

    __slots__ = ("_conn", "_schema", "_register")

    def __init__(
        self, conn: Connection, schema: str, register: Register
    ) -> None:
        self._conn = conn
        self._schema = schema
        self._register = register

    # ── Write operations ──────────────────────────────────────────

    def post(
        self, data: dict[str, Any] | list[dict[str, Any]]
    ) -> int:
        """Post one or more movements. Returns inserted count."""
        return _post(self._conn, self._schema, self._register, data)

    def unpost(self, recorder: str) -> int:
        """Cancel all movements by recorder. Returns deleted count."""
        return _unpost(self._conn, self._schema, self._register, recorder)

    def repost(
        self,
        recorder: str,
        data: dict[str, Any] | list[dict[str, Any]],
    ) -> int:
        """Atomic re-post (unpost old + post new). Returns new count."""
        return _repost(
            self._conn, self._schema, self._register, recorder, data
        )

    # ── Read operations ───────────────────────────────────────────

    def balance(
        self,
        at_date: str | None = None,
        **dims: Any,
    ) -> dict[str, Decimal] | None:
        """Query current or historical balance.

        Dimension filters are passed as keyword arguments::

            handle.balance(warehouse=1, product=42)
            handle.balance(warehouse=1, at_date="2026-01-01")
        """
        options = BalanceOptions(at_date=at_date) if at_date else None
        dim_filter = dims or None
        return _balance(
            self._conn, self._schema, self._register, dim_filter, options
        )

    def turnover(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        group_by: list[str] | None = None,
        **dims: Any,
    ) -> list[dict[str, Any]]:
        """Query turnover for a period.

        Dimension filters are passed as keyword arguments::

            handle.turnover(warehouse=1, date_from="2026-01-01", date_to="2026-03-31")
        """
        options = TurnoverOptions(
            date_from=date_from, date_to=date_to, group_by=group_by
        )
        dim_filter = dims or None
        return _turnover(
            self._conn, self._schema, self._register, dim_filter, options
        )

    def movements(
        self,
        recorder: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **dims: Any,
    ) -> list[dict[str, Any]]:
        """Query movements with filters and pagination.

        Dimension filters are passed as keyword arguments::

            handle.movements(warehouse=1, product=42, limit=50)
        """
        options = MovementsOptions(
            recorder=recorder,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
        dim_filter = dims or None
        return _movements(
            self._conn, self._schema, self._register, dim_filter, options
        )

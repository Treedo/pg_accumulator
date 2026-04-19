"""AccumulatorClient — main entry point for pg_accumulator operations."""

from __future__ import annotations

from typing import Any, Union

from sqlalchemy import Connection, Engine
from sqlalchemy.orm import Session

from .handle import RegisterHandle
from .operations.ddl import (
    alter_register as _alter_register,
    create_register as _create_register,
    drop_register as _drop_register,
    list_registers as _list_registers,
    register_info as _register_info,
)
from .types import (
    AlterRegisterOptions,
    Register,
    RegisterInfo,
    RegisterListRow,
)
from .validation import validate_identifier, validate_register_name

# Anything that can provide a Connection
Connectable = Union[Engine, Session, Connection]


class AccumulatorClient:
    """Type-safe client for pg_accumulator, designed to work alongside SQLAlchemy.

    Accepts an ``Engine``, ``Session``, or ``Connection`` as its backend.

    * When constructed with an ``Engine`` — each operation acquires and releases
      a connection automatically (``engine.begin()``).
    * When constructed with a ``Session`` — operations execute within the
      session's current transaction.
    * When constructed with a ``Connection`` — operations execute directly.

    Example::

        from sqlalchemy import create_engine
        from sqlalchemy_accumulator import AccumulatorClient, define_register

        engine = create_engine("postgresql://localhost/mydb")
        accum = AccumulatorClient(engine)
        accum.use(inventory).post({...})
    """

    __slots__ = ("_backend", "_schema")

    def __init__(
        self,
        backend: Connectable,
        schema: str = "accum",
    ) -> None:
        validate_identifier(schema, "schema")
        self._backend = backend
        self._schema = schema

    # ── Internal: obtain a Connection ─────────────────────────────

    def _is_engine(self) -> bool:
        return isinstance(self._backend, Engine)

    def _get_connection(self) -> Connection:
        """Return a Connection from the backend.

        For Engine — the caller is responsible for using this inside a
        ``with engine.connect()`` block. For Session/Connection — returns
        the underlying connection. For any other object with ``execute``
        (e.g. a raw connection), it is returned directly.
        """
        if isinstance(self._backend, Session):
            return self._backend.connection()
        if isinstance(self._backend, Connection):
            return self._backend
        # Duck-type: anything with .execute() is treated as a connection.
        if hasattr(self._backend, "execute"):
            return self._backend  # type: ignore[return-value]
        raise TypeError(
            "Cannot obtain a Connection from Engine without a context manager. "
            "Use AccumulatorClient(session) or AccumulatorClient(connection) instead, "
            "or wrap calls in engine.connect()."
        )

    def _with_connection(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn(conn, schema, ...)* with a properly scoped connection."""
        if self._is_engine():
            with self._backend.connect() as conn:  # type: ignore[union-attr]
                result = fn(conn, self._schema, *args, **kwargs)
                conn.commit()
                return result
        conn = self._get_connection()
        return fn(conn, self._schema, *args, **kwargs)

    # ── Register handle ───────────────────────────────────────────

    def use(self, register: Register) -> RegisterHandle:
        """Select a register for operations.

        Returns a :class:`RegisterHandle` with ``post``, ``unpost``,
        ``balance``, ``turnover``, and ``movements`` methods.

        When the client is backed by an Engine, each operation on the
        returned handle will auto-acquire a connection.
        """
        validate_register_name(register._def.name)
        if self._is_engine():
            return _EngineRegisterHandle(self._backend, self._schema, register)  # type: ignore[arg-type]
        conn = self._get_connection()
        return RegisterHandle(conn, self._schema, register)

    # ── DDL operations ────────────────────────────────────────────

    def create_register(self, register: Register) -> None:
        """Create a register in the database."""
        validate_register_name(register._def.name)
        self._with_connection(_create_register, register)

    def alter_register(
        self,
        name: str,
        *,
        add_dimensions: dict[str, str] | None = None,
        add_resources: dict[str, str] | None = None,
        high_write: bool | None = None,
    ) -> None:
        """Alter an existing register."""
        validate_register_name(name)
        opts = AlterRegisterOptions(
            add_dimensions=add_dimensions,
            add_resources=add_resources,
            high_write=high_write,
        )
        self._with_connection(_alter_register, name, opts)

    def drop_register(self, name: str, *, force: bool = False) -> None:
        """Drop a register and all its data."""
        validate_register_name(name)
        self._with_connection(_drop_register, name, force)

    def list_registers(self) -> list[RegisterListRow]:
        """List all registers in the database."""
        return self._with_connection(_list_registers)

    def register_info(self, name: str) -> RegisterInfo:
        """Get detailed information about a register."""
        validate_register_name(name)
        return self._with_connection(_register_info, name)


class _EngineRegisterHandle(RegisterHandle):
    """RegisterHandle that auto-manages connection lifecycle via an Engine."""

    def __init__(self, engine: Engine, schema: str, register: Register) -> None:
        self._engine = engine
        # Initialize parent with a placeholder — we override all methods.
        super().__init__(None, schema, register)  # type: ignore[arg-type]

    def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        with self._engine.connect() as conn:
            result = fn(conn, self._schema, self._register, *args, **kwargs)
            conn.commit()
            return result

    def post(self, data: dict[str, Any] | list[dict[str, Any]]) -> int:
        from .operations.post import post as _post
        return self._run(_post, data)

    def unpost(self, recorder: str) -> int:
        from .operations.post import unpost as _unpost
        return self._run(_unpost, recorder)

    def repost(self, recorder: str, data: dict[str, Any] | list[dict[str, Any]]) -> int:
        from .operations.post import repost as _repost
        return self._run(_repost, recorder, data)

    def balance(self, at_date: str | None = None, **dims: Any) -> Any:
        from .operations.read import balance as _balance
        from .types import BalanceOptions
        options = BalanceOptions(at_date=at_date) if at_date else None
        dim_filter = dims or None
        with self._engine.connect() as conn:
            return _balance(conn, self._schema, self._register, dim_filter, options)

    def turnover(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        group_by: list[str] | None = None,
        **dims: Any,
    ) -> list[dict[str, Any]]:
        from .operations.read import turnover as _turnover
        from .types import TurnoverOptions
        options = TurnoverOptions(date_from=date_from, date_to=date_to, group_by=group_by)
        dim_filter = dims or None
        with self._engine.connect() as conn:
            return _turnover(conn, self._schema, self._register, dim_filter, options)

    def movements(
        self,
        recorder: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **dims: Any,
    ) -> list[dict[str, Any]]:
        from .operations.read import movements as _movements
        from .types import MovementsOptions
        options = MovementsOptions(
            recorder=recorder, date_from=date_from, date_to=date_to,
            limit=limit, offset=offset,
        )
        dim_filter = dims or None
        with self._engine.connect() as conn:
            return _movements(conn, self._schema, self._register, dim_filter, options)

"""Read operations: balance, turnover, movements."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ..errors import map_pg_error
from ..types import Register, BalanceOptions, TurnoverOptions, MovementsOptions
from ..validation import sql_identifier, to_timestamp


def balance(
    conn: Connection,
    schema: str,
    register: Register,
    dims: dict[str, Any] | None = None,
    options: BalanceOptions | None = None,
) -> dict[str, Decimal] | None:
    """Query current or historical balance.

    Returns a dict of ``{resource_name: value}`` or ``None`` if no data found.
    """
    name = register._def.name
    fn_name = f"{name}_balance"

    params: dict[str, Any] = {}
    parts: list[str] = []

    if dims:
        parts.append("dimensions := :dims::jsonb")
        params["dims"] = json.dumps(dims, default=str)

    if options and options.at_date is not None:
        parts.append("at_date := :at_date::timestamptz")
        params["at_date"] = to_timestamp(options.at_date)

    arg_list = ", ".join(parts)
    sql = f'SELECT * FROM "{schema}".{sql_identifier(fn_name)}({arg_list})'

    try:
        result = conn.execute(text(sql), params)
        row = result.mappings().fetchone()
        if not row:
            return None

        # The PG function returns a single JSONB column named after the function,
        # or flattened resource columns. Handle both cases.
        if fn_name in row:
            val = row[fn_name]
            if isinstance(val, str):
                parsed = json.loads(val)
                return {k: Decimal(str(v)) for k, v in parsed.items()}
            if isinstance(val, dict):
                return {k: Decimal(str(v)) for k, v in val.items()}
            return None

        # Flattened columns — extract resource keys
        return {
            k: Decimal(str(row[k]))
            for k in register._def.resources
            if k in row
        }
    except Exception as exc:
        map_pg_error(exc)
        raise


def turnover(
    conn: Connection,
    schema: str,
    register: Register,
    dims: dict[str, Any] | None = None,
    options: TurnoverOptions | None = None,
) -> list[dict[str, Any]]:
    """Query turnover for a period. Returns list of result dicts."""
    name = register._def.name
    fn_name = f"{name}_turnover"

    params: dict[str, Any] = {}
    parts: list[str] = []

    if options and options.date_from is not None:
        parts.append("from_date := :date_from::timestamptz")
        params["date_from"] = to_timestamp(options.date_from)

    if options and options.date_to is not None:
        parts.append("to_date := :date_to::timestamptz")
        params["date_to"] = to_timestamp(options.date_to)

    if dims:
        parts.append("dimensions := :dims::jsonb")
        params["dims"] = json.dumps(dims, default=str)

    if options and options.group_by:
        parts.append("group_by := :group_by::jsonb")
        params["group_by"] = json.dumps(options.group_by)

    arg_list = ", ".join(parts)
    sql = f'SELECT * FROM "{schema}".{sql_identifier(fn_name)}({arg_list})'

    try:
        result = conn.execute(text(sql), params)
        rows = result.mappings().fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            if fn_name in row:
                val = row[fn_name]
                if isinstance(val, str):
                    out.append(json.loads(val))
                elif isinstance(val, dict):
                    out.append(val)
                else:
                    out.append(dict(row))
            else:
                out.append(dict(row))
        return out
    except Exception as exc:
        map_pg_error(exc)
        raise


def movements(
    conn: Connection,
    schema: str,
    register: Register,
    dims: dict[str, Any] | None = None,
    options: MovementsOptions | None = None,
) -> list[dict[str, Any]]:
    """Query movements with filters and pagination."""
    name = register._def.name
    fn_name = f"{name}_movements"

    params: dict[str, Any] = {}
    parts: list[str] = []

    if options and options.recorder is not None:
        parts.append("p_recorder := :recorder")
        params["recorder"] = options.recorder

    if options and options.date_from is not None:
        parts.append("from_date := :date_from::timestamptz")
        params["date_from"] = to_timestamp(options.date_from)

    if options and options.date_to is not None:
        parts.append("to_date := :date_to::timestamptz")
        params["date_to"] = to_timestamp(options.date_to)

    if dims:
        parts.append("dimensions := :dims::jsonb")
        params["dims"] = json.dumps(dims, default=str)

    arg_list = ", ".join(parts)
    sql = f'SELECT * FROM "{schema}".{sql_identifier(fn_name)}({arg_list})'

    if options and options.limit is not None:
        sql += f" LIMIT {int(options.limit)}"
    if options and options.offset is not None:
        sql += f" OFFSET {int(options.offset)}"

    try:
        result = conn.execute(text(sql), params)
        rows = result.mappings().fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            if fn_name in row:
                val = row[fn_name]
                if isinstance(val, str):
                    out.append(json.loads(val))
                elif isinstance(val, dict):
                    out.append(val)
                else:
                    out.append(dict(row))
            else:
                out.append(dict(row))
        return out
    except Exception as exc:
        map_pg_error(exc)
        raise

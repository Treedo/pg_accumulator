"""DDL operations: create, alter, drop, list, info."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ..errors import map_pg_error
from ..types import Register, AlterRegisterOptions, RegisterInfo, RegisterListRow


def create_register(conn: Connection, schema: str, register: Register) -> None:
    """Create a register in the database."""
    d = register._def
    params: dict[str, Any] = {
        "name": d.name,
        "dimensions": json.dumps(d.dimensions),
        "resources": json.dumps(d.resources),
        "kind": d.kind,
    }
    parts = [
        "name := :name",
        "dimensions := CAST(:dimensions AS jsonb)",
        "resources := CAST(:resources AS jsonb)",
        "kind := :kind",
    ]

    if d.totals_period != "day":
        parts.append("totals_period := :totals_period")
        params["totals_period"] = d.totals_period

    if d.partition_by != "month":
        parts.append("partition_by := :partition_by")
        params["partition_by"] = d.partition_by

    if d.high_write:
        parts.append("high_write := :high_write")
        params["high_write"] = d.high_write

    if d.recorder_type != "text":
        parts.append("recorder_type := :recorder_type")
        params["recorder_type"] = d.recorder_type

    sql = f'SELECT "{schema}".register_create({", ".join(parts)})'

    try:
        conn.execute(text(sql), params)
    except Exception as exc:
        map_pg_error(exc)
        raise


def alter_register(
    conn: Connection,
    schema: str,
    name: str,
    options: AlterRegisterOptions,
) -> None:
    """Alter an existing register."""
    params: dict[str, Any] = {"name": name}
    parts = ["p_name := :name"]

    if options.add_dimensions is not None:
        parts.append("add_dimensions := CAST(:add_dimensions AS jsonb)")
        params["add_dimensions"] = json.dumps(options.add_dimensions)

    if options.add_resources is not None:
        parts.append("add_resources := CAST(:add_resources AS jsonb)")
        params["add_resources"] = json.dumps(options.add_resources)

    if options.high_write is not None:
        parts.append("high_write := :high_write")
        params["high_write"] = options.high_write

    sql = f'SELECT "{schema}".register_alter({", ".join(parts)})'

    try:
        conn.execute(text(sql), params)
    except Exception as exc:
        map_pg_error(exc)
        raise


def drop_register(
    conn: Connection,
    schema: str,
    name: str,
    force: bool = False,
) -> None:
    """Drop a register and all its data."""
    try:
        conn.execute(
            text(f'SELECT "{schema}".register_drop(:name, :force)'),
            {"name": name, "force": force},
        )
    except Exception as exc:
        map_pg_error(exc)
        raise


def list_registers(conn: Connection, schema: str) -> list[RegisterListRow]:
    """List all registers."""
    try:
        result = conn.execute(text(f'SELECT * FROM "{schema}".register_list()'))
        rows = result.mappings().fetchall()
        return [
            RegisterListRow(
                name=row["name"],
                kind=row["kind"],
                dimensions=int(row["dimensions"]),
                resources=int(row["resources"]),
                movements_count=int(row["movements_count"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]
    except Exception as exc:
        map_pg_error(exc)
        raise


def register_info(conn: Connection, schema: str, name: str) -> RegisterInfo:
    """Get detailed information about a register."""
    try:
        result = conn.execute(
            text(f'SELECT * FROM "{schema}".register_info(:name)'),
            {"name": name},
        )
        row = result.mappings().fetchone()
        if not row:
            from ..errors import RegisterNotFoundError
            raise RegisterNotFoundError(name)

        dims = row.get("dimensions", {})
        if isinstance(dims, str):
            dims = json.loads(dims)

        res = row.get("resources", {})
        if isinstance(res, str):
            res = json.loads(res)

        return RegisterInfo(
            name=row["name"],
            kind=row["kind"],
            dimensions=dims,
            resources=res,
            totals_period=row.get("totals_period", "day"),
            partition_by=row.get("partition_by", "month"),
            high_write=bool(row.get("high_write", False)),
            recorder_type=str(row.get("recorder_type", "text")),
            created_at=str(row.get("created_at", "")),
            movements_count=int(row.get("movements_count", 0)),
            tables=row.get("tables", {}),
            partitions=row.get("partitions"),
        )
    except Exception as exc:
        map_pg_error(exc)
        raise

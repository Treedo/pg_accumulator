"""Write operations: post, unpost, repost."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ..errors import map_pg_error
from ..types import Register
from ..validation import validate_movement


def post(
    conn: Connection,
    schema: str,
    register: Register,
    data: dict[str, Any] | list[dict[str, Any]],
) -> int:
    """Post one or more movements to a register.

    Returns the number of inserted movements.
    """
    movements = data if isinstance(data, list) else [data]
    for m in movements:
        validate_movement(m, register)

    json_data = json.dumps(data if isinstance(data, list) else data, default=str)

    try:
        result = conn.execute(
            text(f'SELECT "{schema}".register_post(:name, :data::jsonb) AS count'),
            {"name": register._def.name, "data": json_data},
        )
        row = result.mappings().fetchone()
        return int(row["count"]) if row else 0
    except Exception as exc:
        map_pg_error(exc)
        raise  # unreachable — map_pg_error always raises


def unpost(
    conn: Connection,
    schema: str,
    register: Register,
    recorder: str,
) -> int:
    """Cancel all movements by recorder. Returns deleted count."""
    try:
        result = conn.execute(
            text(f'SELECT "{schema}".register_unpost(:name, :recorder) AS count'),
            {"name": register._def.name, "recorder": recorder},
        )
        row = result.mappings().fetchone()
        return int(row["count"]) if row else 0
    except Exception as exc:
        map_pg_error(exc)
        raise


def repost(
    conn: Connection,
    schema: str,
    register: Register,
    recorder: str,
    data: dict[str, Any] | list[dict[str, Any]],
) -> int:
    """Atomic re-post: unpost old + post new. Returns new movement count."""
    movements = data if isinstance(data, list) else [data]
    for m in movements:
        validate_movement(m, register)

    json_data = json.dumps(data if isinstance(data, list) else data, default=str)

    try:
        result = conn.execute(
            text(
                f'SELECT "{schema}".register_repost(:name, :recorder, :data::jsonb) AS count'
            ),
            {"name": register._def.name, "recorder": recorder, "data": json_data},
        )
        row = result.mappings().fetchone()
        return int(row["count"]) if row else 0
    except Exception as exc:
        map_pg_error(exc)
        raise

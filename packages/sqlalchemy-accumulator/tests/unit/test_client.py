"""Tests for AccumulatorClient and RegisterHandle."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlalchemy_accumulator import (
    AccumulatorClient,
    RegisterHandle,
    define_register,
    ValidationError,
)


# ── Helpers ───────────────────────────────────────────────────────────

def _make_inventory():
    return define_register(
        name="inventory",
        kind="balance",
        dimensions={"warehouse": "int", "product": "int"},
        resources={"quantity": "numeric", "amount": "numeric"},
    )


def _make_sales():
    return define_register(
        name="sales",
        kind="turnover",
        dimensions={"customer": "int", "product": "int"},
        resources={"quantity": "numeric", "revenue": "numeric"},
    )


def _mock_connection(return_value: Any = None) -> MagicMock:
    """Create a mock SQLAlchemy Connection."""
    conn = MagicMock()
    result = MagicMock()

    if return_value is not None:
        mappings = MagicMock()
        if isinstance(return_value, list):
            mappings.fetchall.return_value = return_value
            mappings.fetchone.return_value = return_value[0] if return_value else None
        else:
            mappings.fetchone.return_value = return_value
            mappings.fetchall.return_value = [return_value]
        result.mappings.return_value = mappings
    else:
        mappings = MagicMock()
        mappings.fetchone.return_value = None
        mappings.fetchall.return_value = []
        result.mappings.return_value = mappings

    conn.execute.return_value = result
    return conn


# ── Client construction ──────────────────────────────────────────────

class TestAccumulatorClient:
    def test_default_schema(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        assert client._schema == "accum"

    def test_custom_schema(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn, schema="my_schema")
        assert client._schema == "my_schema"

    def test_invalid_schema_rejected(self):
        conn = _mock_connection()
        with pytest.raises(ValidationError):
            AccumulatorClient(conn, schema="Robert'; DROP TABLE")

    def test_use_returns_handle(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        inventory = _make_inventory()
        handle = client.use(inventory)
        assert isinstance(handle, RegisterHandle)

    def test_use_rejects_invalid_name(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        bad_reg = define_register(
            name="BAD NAME",
            kind="balance",
            dimensions={"x": "int"},
            resources={"y": "numeric"},
        )
        with pytest.raises(ValidationError):
            client.use(bad_reg)


# ── Post operations ──────────────────────────────────────────────────

class TestPost:
    def test_single_post(self):
        conn = _mock_connection({"count": 1})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        count = client.use(inventory).post({
            "recorder": "purchase:7001",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 100,
            "amount": 5000,
        })

        assert count == 1
        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "register_post" in sql_text
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1]
        assert params["name"] == "inventory"

    def test_batch_post(self):
        conn = _mock_connection({"count": 2})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        data = [
            {"recorder": "p:1", "period": "2026-04-19", "warehouse": 1, "product": 42,
             "quantity": 50, "amount": 2500},
            {"recorder": "p:1", "period": "2026-04-19", "warehouse": 1, "product": 43,
             "quantity": 200, "amount": 8000},
        ]
        count = client.use(inventory).post(data)
        assert count == 2

    def test_post_validates_movement(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        with pytest.raises(ValidationError):
            client.use(inventory).post({
                "period": "2026-04-19",
                "warehouse": 1,
                "product": 42,
                # missing recorder
            })

    def test_post_validates_dimensions(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        with pytest.raises(ValidationError):
            client.use(inventory).post({
                "recorder": "x",
                "period": "2026-04-19",
                "warehouse": 1,
                # missing product
            })


# ── Unpost ────────────────────────────────────────────────────────────

class TestUnpost:
    def test_unpost(self):
        conn = _mock_connection({"count": 3})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        count = client.use(inventory).unpost("purchase:7001")
        assert count == 3

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "register_unpost" in sql_text


# ── Repost ────────────────────────────────────────────────────────────

class TestRepost:
    def test_repost(self):
        conn = _mock_connection({"count": 1})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        count = client.use(inventory).repost("purchase:7001", {
            "recorder": "purchase:7001",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 120,
            "amount": 6000,
        })
        assert count == 1

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "register_repost" in sql_text


# ── Balance ───────────────────────────────────────────────────────────

class TestBalance:
    def test_balance_no_filters(self):
        conn = _mock_connection({"inventory_balance": '{"quantity": 100, "amount": 5000}'})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).balance()
        assert result is not None
        assert result["quantity"] == Decimal("100")
        assert result["amount"] == Decimal("5000")

    def test_balance_with_dims(self):
        conn = _mock_connection({"inventory_balance": '{"quantity": 50, "amount": 2500}'})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).balance(warehouse=1, product=42)
        assert result is not None
        assert result["quantity"] == Decimal("50")

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "dimensions" in sql_text

    def test_balance_with_at_date(self):
        conn = _mock_connection({"inventory_balance": '{"quantity": 0, "amount": 0}'})
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).balance(warehouse=1, at_date="2026-01-01")
        assert result is not None

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "at_date" in sql_text

    def test_balance_returns_none_when_no_data(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).balance(warehouse=999)
        assert result is None


# ── Turnover ──────────────────────────────────────────────────────────

class TestTurnover:
    def test_turnover_with_period(self):
        row = {"inventory_turnover": '{"quantity": 300, "amount": 15000}'}
        conn = _mock_connection([row])
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).turnover(
            warehouse=1,
            date_from="2026-01-01",
            date_to="2026-03-31",
        )
        assert len(result) == 1

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "from_date" in sql_text
        assert "to_date" in sql_text


# ── Movements ─────────────────────────────────────────────────────────

class TestMovements:
    def test_movements_with_limit(self):
        row = {
            "id": "abc-123",
            "recorder": "purchase:1",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 100,
            "amount": 5000,
        }
        conn = _mock_connection([row])
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        result = client.use(inventory).movements(
            warehouse=1, product=42, limit=50,
        )
        assert len(result) == 1

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "LIMIT 50" in sql_text


# ── DDL ───────────────────────────────────────────────────────────────

class TestDDL:
    def test_create_register(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)
        inventory = _make_inventory()

        client.create_register(inventory)

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "register_create" in sql_text

    def test_list_registers(self):
        rows = [
            {
                "name": "inventory",
                "kind": "balance",
                "dimensions": 2,
                "resources": 2,
                "movements_count": 100,
                "created_at": "2026-04-19",
            }
        ]
        conn = _mock_connection(rows)
        client = AccumulatorClient(conn)

        result = client.list_registers()
        assert len(result) == 1
        assert result[0].name == "inventory"
        assert result[0].kind == "balance"

    def test_drop_register(self):
        conn = _mock_connection()
        client = AccumulatorClient(conn)

        client.drop_register("inventory")

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert "register_drop" in sql_text


# ── Session integration ──────────────────────────────────────────────

class TestSessionIntegration:
    def test_session_backend(self):
        """AccumulatorClient with a Session mock delegates to session.connection()."""
        from sqlalchemy.orm import Session

        inner_conn = _mock_connection({"count": 1})

        session = MagicMock(spec=Session)
        session.connection.return_value = inner_conn

        client = AccumulatorClient(session)
        inventory = _make_inventory()

        count = client.use(inventory).post({
            "recorder": "test:1",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 10,
            "amount": 100,
        })
        assert count == 1
        # Session.connection() should have been called
        session.connection.assert_called_once()

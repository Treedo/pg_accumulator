"""Tests for validation helpers."""

import pytest

from sqlalchemy_accumulator.validation import (
    validate_register_name,
    validate_identifier,
    validate_dimensions,
    validate_movement,
    to_timestamp,
    sql_identifier,
)
from sqlalchemy_accumulator.errors import ValidationError
from sqlalchemy_accumulator import define_register


class TestValidateRegisterName:
    def test_valid_names(self):
        for name in ["inventory", "sales_2024", "a", "_private", "abc_def_ghi"]:
            validate_register_name(name)  # should not raise

    def test_invalid_names(self):
        for name in [
            "123abc",       # starts with digit
            "SELECT",       # uppercase
            "my-register",  # hyphen
            "my register",  # space
            "a" * 64,       # too long (63 max)
            "",             # empty
            "Robert'; DROP TABLE students;--",  # SQL injection
        ]:
            with pytest.raises(ValidationError):
                validate_register_name(name)


class TestValidateIdentifier:
    def test_valid_schema(self):
        validate_identifier("accum", "schema")

    def test_invalid_schema(self):
        with pytest.raises(ValidationError):
            validate_identifier("my schema", "schema")


class TestValidateDimensions:
    def test_all_present(self):
        validate_dimensions(
            {"warehouse": 1, "product": 42},
            {"warehouse": "int", "product": "int"},
        )

    def test_missing_dimension(self):
        with pytest.raises(ValidationError) as exc_info:
            validate_dimensions(
                {"warehouse": 1},
                {"warehouse": "int", "product": "int"},
            )
        assert exc_info.value.field == "product"

    def test_none_value_is_missing(self):
        with pytest.raises(ValidationError):
            validate_dimensions(
                {"warehouse": 1, "product": None},
                {"warehouse": "int", "product": "int"},
            )


class TestValidateMovement:
    @pytest.fixture()
    def inventory(self):
        return define_register(
            name="inventory",
            kind="balance",
            dimensions={"warehouse": "int", "product": "int"},
            resources={"quantity": "numeric"},
        )

    def test_valid_movement(self, inventory):
        validate_movement(
            {
                "recorder": "order:1",
                "period": "2026-04-19",
                "warehouse": 1,
                "product": 42,
                "quantity": 100,
            },
            inventory,
        )

    def test_missing_recorder(self, inventory):
        with pytest.raises(ValidationError) as exc_info:
            validate_movement(
                {"period": "2026-04-19", "warehouse": 1, "product": 42},
                inventory,
            )
        assert exc_info.value.field == "recorder"

    def test_missing_period(self, inventory):
        with pytest.raises(ValidationError) as exc_info:
            validate_movement(
                {"recorder": "x", "warehouse": 1, "product": 42},
                inventory,
            )
        assert exc_info.value.field == "period"

    def test_missing_dimension(self, inventory):
        with pytest.raises(ValidationError) as exc_info:
            validate_movement(
                {"recorder": "x", "period": "2026-04-19", "warehouse": 1},
                inventory,
            )
        assert exc_info.value.field == "product"


class TestToTimestamp:
    def test_string_passthrough(self):
        assert to_timestamp("2026-04-19") == "2026-04-19"

    def test_date_object(self):
        from datetime import date
        assert to_timestamp(date(2026, 4, 19)) == "2026-04-19"

    def test_datetime_object(self):
        from datetime import datetime
        dt = datetime(2026, 4, 19, 10, 30, 0)
        assert "2026-04-19" in to_timestamp(dt)


class TestSqlIdentifier:
    def test_valid(self):
        assert sql_identifier("inventory") == '"inventory"'

    def test_invalid_raises(self):
        with pytest.raises(ValidationError):
            sql_identifier("Robert'; DROP TABLE students;--")

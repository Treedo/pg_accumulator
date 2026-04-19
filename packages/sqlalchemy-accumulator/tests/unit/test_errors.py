"""Tests for error classes and pg error mapping."""

import pytest

from sqlalchemy_accumulator.errors import (
    AccumulatorError,
    RegisterNotFoundError,
    RecorderNotFoundError,
    RegisterExistsError,
    ValidationError,
    map_pg_error,
)


class TestErrorHierarchy:
    def test_register_not_found_is_accumulator_error(self):
        err = RegisterNotFoundError("inventory")
        assert isinstance(err, AccumulatorError)
        assert err.register_name == "inventory"
        assert "inventory" in str(err)

    def test_recorder_not_found_is_accumulator_error(self):
        err = RecorderNotFoundError("order:1")
        assert isinstance(err, AccumulatorError)
        assert err.recorder == "order:1"

    def test_register_exists_is_accumulator_error(self):
        err = RegisterExistsError("inventory")
        assert isinstance(err, AccumulatorError)
        assert err.register_name == "inventory"

    def test_validation_error_with_field(self):
        err = ValidationError("bad value", field="name")
        assert isinstance(err, AccumulatorError)
        assert err.field == "name"

    def test_validation_error_without_field(self):
        err = ValidationError("bad value")
        assert err.field is None


class TestMapPgError:
    def test_register_does_not_exist(self):
        pg_err = Exception('Register "inventory" does not exist')
        with pytest.raises(RegisterNotFoundError) as exc_info:
            map_pg_error(pg_err)
        assert exc_info.value.register_name == "inventory"

    def test_register_not_found(self):
        pg_err = Exception('register "sales" not found')
        with pytest.raises(RegisterNotFoundError):
            map_pg_error(pg_err)

    def test_register_already_exists(self):
        pg_err = Exception('Register "inventory" already exists')
        with pytest.raises(RegisterExistsError) as exc_info:
            map_pg_error(pg_err)
        assert exc_info.value.register_name == "inventory"

    def test_recorder_not_found(self):
        pg_err = Exception('Recorder "order:99" not found')
        with pytest.raises(RecorderNotFoundError) as exc_info:
            map_pg_error(pg_err)
        assert exc_info.value.recorder == "order:99"

    def test_unknown_error_passthrough(self):
        pg_err = ValueError("something else entirely")
        with pytest.raises(ValueError):
            map_pg_error(pg_err)

    def test_generic_exception_passthrough(self):
        pg_err = RuntimeError("connection refused")
        with pytest.raises(RuntimeError):
            map_pg_error(pg_err)

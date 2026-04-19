"""Tests for define_register() and Register types."""

from sqlalchemy_accumulator import define_register


class TestDefineRegister:
    def test_basic_balance_register(self):
        reg = define_register(
            name="inventory",
            kind="balance",
            dimensions={"warehouse": "int", "product": "int"},
            resources={"quantity": "numeric", "amount": "numeric"},
        )
        assert reg._def.name == "inventory"
        assert reg._def.kind == "balance"
        assert reg._def.dimensions == {"warehouse": "int", "product": "int"}
        assert reg._def.resources == {"quantity": "numeric", "amount": "numeric"}
        assert reg._def.totals_period == "day"
        assert reg._def.partition_by == "month"
        assert reg._def.high_write is False
        assert reg._def.recorder_type == "text"

    def test_turnover_register(self):
        reg = define_register(
            name="sales",
            kind="turnover",
            dimensions={"customer": "int", "product": "int"},
            resources={"quantity": "numeric", "revenue": "numeric"},
        )
        assert reg._def.kind == "turnover"

    def test_optional_fields_preserved(self):
        reg = define_register(
            name="test_reg",
            kind="balance",
            dimensions={"dim": "int"},
            resources={"res": "numeric"},
            totals_period="month",
            partition_by="quarter",
            high_write=True,
            recorder_type="uuid",
        )
        assert reg._def.totals_period == "month"
        assert reg._def.partition_by == "quarter"
        assert reg._def.high_write is True
        assert reg._def.recorder_type == "uuid"

    def test_definition_is_frozen(self):
        reg = define_register(
            name="test",
            kind="balance",
            dimensions={"x": "int"},
            resources={"y": "numeric"},
        )
        import dataclasses

        assert dataclasses.is_dataclass(reg._def)
        # Frozen dataclass — cannot assign attributes
        try:
            reg._def.name = "hacked"  # type: ignore[misc]
            assert False, "Should have raised FrozenInstanceError"
        except dataclasses.FrozenInstanceError:
            pass

    def test_dimensions_are_copied(self):
        original_dims = {"a": "int"}
        reg = define_register(
            name="test",
            kind="balance",
            dimensions=original_dims,
            resources={"r": "numeric"},
        )
        # Mutating original should not affect register
        original_dims["b"] = "text"
        assert "b" not in reg._def.dimensions

"""Register definition factory."""

from __future__ import annotations

from .types import Register, RegisterDefinition, RegisterKind, TotalsPeriod, PartitionBy, RecorderType


def define_register(
    *,
    name: str,
    kind: RegisterKind,
    dimensions: dict[str, str],
    resources: dict[str, str],
    totals_period: TotalsPeriod = "day",
    partition_by: PartitionBy = "month",
    high_write: bool = False,
    recorder_type: RecorderType = "text",
) -> Register:
    """Create a typed accumulation register definition.

    Returns a ``Register`` handle to be used with :class:`AccumulatorClient`.
    """
    return Register(
        _def=RegisterDefinition(
            name=name,
            kind=kind,
            dimensions=dict(dimensions),
            resources=dict(resources),
            totals_period=totals_period,
            partition_by=partition_by,
            high_write=high_write,
            recorder_type=recorder_type,
        )
    )

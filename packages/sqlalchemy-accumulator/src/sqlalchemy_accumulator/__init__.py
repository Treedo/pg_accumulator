"""sqlalchemy-accumulator — SQLAlchemy adapter for pg_accumulator."""

from .client import AccumulatorClient
from .errors import (
    AccumulatorError,
    RecorderNotFoundError,
    RegisterExistsError,
    RegisterNotFoundError,
    ValidationError,
)
from .handle import RegisterHandle
from .register import define_register
from .types import (
    AccumulatorConfig,
    AlterRegisterOptions,
    BalanceOptions,
    MovementsOptions,
    Register,
    RegisterDefinition,
    RegisterInfo,
    RegisterListRow,
    TurnoverOptions,
)

__all__ = [
    # Client
    "AccumulatorClient",
    "RegisterHandle",
    # Factory
    "define_register",
    # Errors
    "AccumulatorError",
    "RegisterNotFoundError",
    "RecorderNotFoundError",
    "RegisterExistsError",
    "ValidationError",
    # Types
    "Register",
    "RegisterDefinition",
    "AccumulatorConfig",
    "BalanceOptions",
    "TurnoverOptions",
    "MovementsOptions",
    "AlterRegisterOptions",
    "RegisterInfo",
    "RegisterListRow",
]

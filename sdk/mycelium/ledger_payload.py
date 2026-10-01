"""Explicit retention policy for ledger call and result payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LedgerPayloadPolicy:
    store_args: bool = True
    store_result: bool = True
    redact_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.store_args) is not bool or type(self.store_result) is not bool:
            raise ValueError("store_args and store_result must be booleans")
        if not isinstance(self.redact_fields, (list, tuple)) or any(
            not isinstance(field, str) or not field for field in self.redact_fields
        ):
            raise ValueError("redact_fields must be a list of non-empty field names")
        object.__setattr__(self, "redact_fields", tuple(self.redact_fields))

    def redact(self, value: Any) -> tuple[Any, bool]:
        """Redact matching mapping keys at any depth, including inside lists."""
        if isinstance(value, dict):
            changed = False
            output = {}
            for key, item in value.items():
                if key in self.redact_fields:
                    output[key] = "[REDACTED]"
                    changed = True
                else:
                    output[key], nested = self.redact(item)
                    changed |= nested
            return output, changed
        if isinstance(value, (list, tuple)):
            parts = [self.redact(item) for item in value]
            items = [item for item, _ in parts]
            return (
                tuple(items) if isinstance(value, tuple) else items,
                any(changed for _, changed in parts),
            )
        return value, False

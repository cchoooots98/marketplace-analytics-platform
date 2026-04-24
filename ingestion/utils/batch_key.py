"""Shared batch-key contract for ingestion discovery and state tracking."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatchKey:
    """Stable identifier for one source batch file across the control plane.

    Args:
        source_name: Human-readable source name such as ``orders``.
        batch_id: Stable batch identifier from the landing contract.
        source_file_name: File name consumed from the source batch directory.
    """

    source_name: str
    batch_id: str
    source_file_name: str

    def __post_init__(self) -> None:
        """Validate the key so control-plane state stays deterministic."""
        for field_name, field_value in (
            ("source_name", self.source_name),
            ("batch_id", self.batch_id),
            ("source_file_name", self.source_file_name),
        ):
            if not field_value.strip():
                msg = f"{field_name} cannot be empty"
                raise ValueError(msg)

    def to_dict(self) -> dict[str, str]:
        """Convert the key into a JSON-safe dictionary."""
        return {
            "source_name": self.source_name,
            "batch_id": self.batch_id,
            "source_file_name": self.source_file_name,
        }

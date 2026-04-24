"""Shared ingestion summary models."""

from __future__ import annotations

from dataclasses import dataclass

from ingestion.olist.batch_runtime import DateWindow


@dataclass(frozen=True)
class LoadedSourceBatch:
    """Summary of one raw batch loaded or marked as published.

    Args:
        source_name: Human-readable source name such as ``orders``.
        table_id: Raw destination BigQuery table.
        loaded_rows: BigQuery-reported loaded row count.
        batch_id: Stable batch identifier from the landing contract, when present.
        source_file_name: Source object name when known.
    """

    source_name: str
    table_id: str
    loaded_rows: int
    batch_id: str | None = None
    source_file_name: str | None = None

    def to_dict(self) -> dict[str, int | str | None]:
        """Convert the consumed batch summary into a JSON-safe dictionary."""
        return {
            "source_name": self.source_name,
            "table_id": self.table_id,
            "loaded_rows": self.loaded_rows,
            "batch_id": self.batch_id,
            "source_file_name": self.source_file_name,
        }


@dataclass(frozen=True)
class IngestionRunSummary:
    """Structured summary for one bootstrap or incremental ingestion run.

    Args:
        mode: Ingestion mode, either ``bootstrap`` or ``incremental``.
        no_op: True when the run made no control-plane progress.
        publish_complete: True when there are no unresolved publish steps after
            the run finishes.
        raw_batches_loaded: Raw source batches loaded by the run.
        batches_marked_published: Batches whose publish status advanced to
            ``published`` during the run.
        holiday_date_window: Derived or requested holiday window.
        weather_date_window: Derived or requested weather window.
    """

    mode: str
    no_op: bool
    publish_complete: bool
    raw_batches_loaded: tuple[LoadedSourceBatch, ...] = ()
    batches_marked_published: tuple[LoadedSourceBatch, ...] = ()
    holiday_date_window: DateWindow | None = None
    weather_date_window: DateWindow | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert the summary into a logging- and Airflow-friendly mapping."""
        return {
            "mode": self.mode,
            "no_op": self.no_op,
            "publish_complete": self.publish_complete,
            "raw_batches_loaded": [
                batch.to_dict() for batch in self.raw_batches_loaded
            ],
            "batches_marked_published": [
                batch.to_dict() for batch in self.batches_marked_published
            ],
            "holiday_date_window": (
                self.holiday_date_window.to_dict()
                if self.holiday_date_window is not None
                else None
            ),
            "weather_date_window": (
                self.weather_date_window.to_dict()
                if self.weather_date_window is not None
                else None
            ),
        }

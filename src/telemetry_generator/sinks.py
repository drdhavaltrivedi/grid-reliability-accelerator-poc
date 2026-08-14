"""Pluggable output sinks for the telemetry generator.

The generator itself is sink-agnostic (spec calls for Structured
Streaming/Auto Loader into a bronze Delta table - Data Sources #1 - but that
needs a live Databricks workspace, which isn't available yet per the
"build locally first" decision). ParquetSink writes micro-batch files to a
directory in the same shape Auto Loader would later pick up via `cloudFiles`
pointed at the equivalent cloud path, so swapping to real bronze ingestion in
Phase 3 is a path change, not a rewrite.
"""
from __future__ import annotations

import csv
import json
import sys
import uuid
from abc import ABC, abstractmethod
from pathlib import Path


class Sink(ABC):
    @abstractmethod
    def write(self, rows: list[dict]) -> None: ...

    def close(self) -> None:
        pass


class ConsoleSink(Sink):
    """Prints each row as JSON. Useful for interactive/demo terminals."""

    def write(self, rows: list[dict]) -> None:
        for row in rows:
            print(json.dumps(row), file=sys.stdout, flush=True)


class JsonlSink(Sink):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def write(self, rows: list[dict]) -> None:
        for row in rows:
            self._fh.write(json.dumps(row) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class CsvSink(Sink):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.path.exists()
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._writer: csv.DictWriter | None = None
        self._write_header = write_header

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        if self._writer is None:
            self._writer = csv.DictWriter(self._fh, fieldnames=list(rows[0].keys()))
            if self._write_header:
                self._writer.writeheader()
        self._writer.writerows(rows)
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class ParquetSink(Sink):
    """Writes one parquet file per flush into `directory`, named so a
    directory listing sorts in write order - the shape Auto Loader's
    `cloudFiles` source expects when pointed at a landing zone.
    """

    def __init__(self, directory: str | Path) -> None:
        try:
            import pandas as pd  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "ParquetSink requires pandas and pyarrow: pip install pandas pyarrow"
            ) from e

        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._batch_num = 0

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        import pandas as pd

        df = pd.DataFrame(rows)
        fname = f"part-{self._batch_num:06d}-{uuid.uuid4().hex[:8]}.parquet"
        df.to_parquet(self.directory / fname, index=False)
        self._batch_num += 1


def build_sink(kind: str, path: str) -> Sink:
    kind = kind.lower()
    if kind == "console":
        return ConsoleSink()
    if kind == "jsonl":
        return JsonlSink(path)
    if kind == "csv":
        return CsvSink(path)
    if kind == "parquet":
        return ParquetSink(path)
    raise ValueError(f"unknown sink kind: {kind!r} (expected console|jsonl|csv|parquet)")

"""Storage backends: SQLite (always available) and InfluxDB (optional)."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Protocol

from .config import InfluxConfig, SqliteConfig

log = logging.getLogger(__name__)


@dataclass
class Measurement:
    ts: int          # unix epoch seconds (UTC)
    sensor: str
    channel: int
    kind: str        # 'live' | 'window_min' | 'window_max' | 'window_avg'
    value: float


class StorageBackend(Protocol):
    def write(self, measurements: Iterable[Measurement]) -> int: ...
    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# SQLite
# --------------------------------------------------------------------------- #

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS measurements (
    ts          INTEGER NOT NULL,
    sensor      TEXT    NOT NULL,
    channel     INTEGER NOT NULL,
    kind        TEXT NOT NULL,      -- 'live' | 'window_min' | 'window_max' | 'window_avg'
    value       REAL    NOT NULL,
    PRIMARY KEY (ts, sensor, channel, kind)
);
CREATE INDEX IF NOT EXISTS idx_measurements_sensor_ts
    ON measurements (sensor, ts);
"""


class SqliteStorage:
    def __init__(self, cfg: SqliteConfig):
        Path(cfg.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(cfg.path, isolation_level=None)
        self._conn.executescript(_SQLITE_SCHEMA)
        log.info("SQLite storage ready at %s", cfg.path)

    def write(self, measurements: Iterable[Measurement]) -> int:
        rows = [(m.ts, m.sensor, m.channel, m.kind, m.value) for m in measurements]
        if not rows:
            return 0
        cur = self._conn.executemany(
            "INSERT OR IGNORE INTO measurements (ts, sensor, channel, kind, value) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        return cur.rowcount or 0

    def close(self) -> None:
        self._conn.close()


# --------------------------------------------------------------------------- #
# InfluxDB v2
# --------------------------------------------------------------------------- #


class InfluxStorage:
    def __init__(self, cfg: InfluxConfig):
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS

        self._client = InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._bucket = cfg.bucket
        self._org = cfg.org
        log.info("InfluxDB storage ready at %s (bucket=%s)", cfg.url, cfg.bucket)

    def write(self, measurements: Iterable[Measurement]) -> int:
        from influxdb_client import Point

        points = []
        for m in measurements:
            p = (
                Point("paxcounter")
                .tag("sensor", m.sensor)
                .tag("channel", str(m.channel))
                .tag("kind", m.kind)
                .field("value", float(m.value))
                .time(m.ts * 1_000_000_000)
            )
            points.append(p)
        if not points:
            return 0
        self._write_api.write(bucket=self._bucket, org=self._org, record=points)
        return len(points)

    def close(self) -> None:
        self._client.close()


# --------------------------------------------------------------------------- #
# Composite
# --------------------------------------------------------------------------- #


class CompositeStorage:
    def __init__(self, backends: List[StorageBackend]):
        self._backends = backends

    def write(self, measurements: Iterable[Measurement]) -> int:
        ms = list(measurements)
        total = 0
        for b in self._backends:
            try:
                total += b.write(ms)
            except Exception:
                log.exception("Storage backend %s failed; continuing", type(b).__name__)
        return total

    def close(self) -> None:
        for b in self._backends:
            try:
                b.close()
            except Exception:
                log.exception("Error closing %s", type(b).__name__)


def build_storage(sqlite_cfg: SqliteConfig, influx_cfg: InfluxConfig) -> Optional[StorageBackend]:
    backends: List[StorageBackend] = []
    if sqlite_cfg.enabled:
        backends.append(SqliteStorage(sqlite_cfg))
    if influx_cfg.enabled:
        backends.append(InfluxStorage(influx_cfg))
    if not backends:
        log.warning("No storage backend enabled - running in dry-run mode")
        return None
    return CompositeStorage(backends)

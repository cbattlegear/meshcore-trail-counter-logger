"""Config loading. TOML in, dataclasses out."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

log = logging.getLogger(__name__)


def _filter_kwargs(cls, raw: Dict[str, Any], where: str) -> Dict[str, Any]:
    """Drop keys that aren't fields of `cls`, warning about each one.

    Keeps old config files working when we rename / remove a setting.
    """
    known = {f.name for f in fields(cls)}
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        if k in known:
            out[k] = v
        else:
            log.warning("Ignoring unknown config key %r in %s", k, where)
    return out


@dataclass
class GatewayConfig:
    transport: str = "serial"
    port: str = ""
    baud: int = 115200


@dataclass
class SensorConfig:
    name: str
    pubkey: str
    secret: str = ""
    poll_interval_seconds: int = 300
    # If > 0, each poll also requests a min/max/avg snapshot over this trailing
    # window. 0 disables. A single MMA call returns ONE aggregate per channel
    # for the entire window (not per-hour samples), so set this to your poll
    # interval to get rolling stats.
    mma_window_seconds: int = 0
    channels: List[int] = field(default_factory=lambda: [1, 2, 3, 4, 5])


@dataclass
class SqliteConfig:
    enabled: bool = True
    path: str = "data/trail.db"


@dataclass
class InfluxConfig:
    enabled: bool = False
    url: str = ""
    token: str = ""
    org: str = ""
    bucket: str = ""


@dataclass
class StorageConfig:
    sqlite: SqliteConfig = field(default_factory=SqliteConfig)
    influxdb: InfluxConfig = field(default_factory=InfluxConfig)


@dataclass
class LoggingConfig:
    level: str = "INFO"


@dataclass
class Config:
    gateway: GatewayConfig
    sensors: List[SensorConfig]
    storage: StorageConfig
    logging: LoggingConfig


def load(path: Optional[Path] = None) -> Config:
    path = Path(path or "config.toml")
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}. "
            "Copy config.example.toml to config.toml and edit it."
        )
    with path.open("rb") as f:
        raw = tomllib.load(f)

    gw = GatewayConfig(**_filter_kwargs(GatewayConfig, raw.get("gateway", {}), "[gateway]"))
    sensors = [
        SensorConfig(**_filter_kwargs(SensorConfig, s, f"[[sensors]] #{i}"))
        for i, s in enumerate(raw.get("sensors", []))
    ]
    if not sensors:
        raise ValueError("config.toml must define at least one [[sensors]] block")

    storage_raw = raw.get("storage", {})
    storage = StorageConfig(
        sqlite=SqliteConfig(**_filter_kwargs(SqliteConfig, storage_raw.get("sqlite", {}), "[storage.sqlite]")),
        influxdb=InfluxConfig(**_filter_kwargs(InfluxConfig, storage_raw.get("influxdb", {}), "[storage.influxdb]")),
    )
    log_cfg = LoggingConfig(**_filter_kwargs(LoggingConfig, raw.get("logging", {}), "[logging]"))

    return Config(gateway=gw, sensors=sensors, storage=storage, logging=log_cfg)

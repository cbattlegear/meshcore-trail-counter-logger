"""Main poller entrypoint.

Connects to a local MeshCore gateway via serial, polls each configured
paxcounter sensor on its own schedule, and writes telemetry into the
configured storage backends.

This is a skeleton: the actual meshcore-py call surface is reached via small
`_poll_live` / `_poll_history` helpers that you may need to tweak as
meshcore-py evolves. Search for `# TODO(meshcore-py)` markers.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from dataclasses import dataclass
from typing import List, Optional

from . import config as _config
from .storage import Measurement, build_storage

log = logging.getLogger(__name__)


async def open_gateway(cfg: _config.GatewayConfig):
    try:
        from meshcore import MeshCore
    except ImportError as e:
        raise SystemExit(
            "meshcore-py is not installed. Run: pip install -r requirements.txt"
        ) from e

    if cfg.transport != "serial":
        raise NotImplementedError(
            f"Transport '{cfg.transport}' not yet supported. Use 'serial'."
        )

    log.info("Connecting to gateway on %s @ %d baud", cfg.port, cfg.baud)
    # TODO(meshcore-py): factory name may differ between versions.
    mc = await MeshCore.create_serial(cfg.port, baudrate=cfg.baud)
    return mc


@dataclass
class SensorState:
    cfg: _config.SensorConfig
    next_poll_at: float = 0.0
    consecutive_failures: int = 0


async def _resolve_contact(mc, pubkey_prefix: str):
    # TODO(meshcore-py): contacts API
    contacts = await mc.commands.get_contacts()
    for c in contacts.values():
        pk = getattr(c, "public_key", "") or ""
        if pk.lower().startswith(pubkey_prefix.lower()):
            return c
    raise LookupError(f"No contact matches pubkey prefix {pubkey_prefix!r}")


async def _login_if_needed(mc, contact, secret: str) -> None:
    if not secret:
        return
    # TODO(meshcore-py): login API
    await mc.commands.send_login(contact, secret)


def _records_from_lpp(sensor_name: str, ts: int, kind: str,
                      channels_wanted: List[int], decoded: list) -> List[Measurement]:
    out: List[Measurement] = []
    for item in decoded:
        ch = item.get("channel")
        if ch is None or (channels_wanted and ch not in channels_wanted):
            continue
        val = item.get("value")
        if isinstance(val, (list, tuple)):
            continue
        try:
            out.append(Measurement(ts=ts, sensor=sensor_name, channel=int(ch),
                                   kind=kind, value=float(val)))
        except (TypeError, ValueError):
            continue
    return out


async def _poll_live(mc, contact, sensor: _config.SensorConfig) -> List[Measurement]:
    # TODO(meshcore-py): telemetry-request helper name
    resp = await mc.commands.send_telemetry_req(contact)
    if not resp or not getattr(resp, "lpp", None):
        return []
    ts = int(time.time())
    return _records_from_lpp(sensor.name, ts, "live", sensor.channels, resp.lpp)


async def _poll_history(mc, contact, sensor: _config.SensorConfig) -> List[Measurement]:
    if sensor.backfill_hours <= 0:
        return []
    start_secs_ago = sensor.backfill_hours * 3600
    end_secs_ago = 0
    # TODO(meshcore-py): min/max/avg helper name
    resp = await mc.commands.send_telemetry_req_minmaxavg(
        contact, start_secs_ago=start_secs_ago, end_secs_ago=end_secs_ago
    )
    if not resp or not getattr(resp, "samples", None):
        return []
    out: List[Measurement] = []
    for sample in resp.samples:
        ts = int(sample["ts"])
        ch = int(sample["channel"])
        if sensor.channels and ch not in sensor.channels:
            continue
        out.append(Measurement(ts, sensor.name, ch, "hourly_min", float(sample["min"])))
        out.append(Measurement(ts, sensor.name, ch, "hourly_max", float(sample["max"])))
        out.append(Measurement(ts, sensor.name, ch, "hourly_avg", float(sample["avg"])))
    return out


async def _poll_sensor(mc, sensor: _config.SensorConfig, storage) -> int:
    contact = await _resolve_contact(mc, sensor.pubkey)
    await _login_if_needed(mc, contact, sensor.secret)

    measurements = []
    measurements.extend(await _poll_live(mc, contact, sensor))
    measurements.extend(await _poll_history(mc, contact, sensor))

    if storage and measurements:
        n = storage.write(measurements)
        log.info("[%s] wrote %d new measurements (of %d)", sensor.name, n, len(measurements))
        return n
    elif measurements:
        log.info("[%s] dry-run: %d measurements (storage disabled)", sensor.name, len(measurements))
    else:
        log.warning("[%s] no measurements returned", sensor.name)
    return 0


async def run(cfg: _config.Config) -> None:
    storage = build_storage(cfg.storage.sqlite, cfg.storage.influxdb)
    mc = await open_gateway(cfg.gateway)

    states = [SensorState(cfg=s) for s in cfg.sensors]
    stop = asyncio.Event()

    def _handle_signal(*_):
        log.info("Shutdown signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, _handle_signal)

    try:
        while not stop.is_set():
            now = time.monotonic()
            for st in states:
                if now < st.next_poll_at:
                    continue
                try:
                    await _poll_sensor(mc, st.cfg, storage)
                    st.consecutive_failures = 0
                except Exception:
                    st.consecutive_failures += 1
                    log.exception("[%s] poll failed (failure #%d)",
                                  st.cfg.name, st.consecutive_failures)
                backoff = min(60 * 60, st.cfg.poll_interval_seconds * max(1, st.consecutive_failures))
                st.next_poll_at = now + backoff

            sleep_for = min((st.next_poll_at - time.monotonic() for st in states), default=5.0)
            sleep_for = max(0.5, min(5.0, sleep_for))
            try:
                await asyncio.wait_for(stop.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("Closing gateway + storage")
        try:
            await mc.disconnect()
        except Exception:
            log.exception("Error disconnecting gateway")
        if storage:
            storage.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MeshCore trail-counter logger")
    parser.add_argument("-c", "--config", default="config.toml",
                        help="Path to config TOML (default: config.toml)")
    args = parser.parse_args(argv)

    cfg = _config.load(args.config)
    logging.basicConfig(
        level=getattr(logging, cfg.logging.level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    asyncio.run(run(cfg))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

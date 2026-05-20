"""Main poller entrypoint.

Connects to a local MeshCore gateway via serial, polls each configured
paxcounter sensor on its own schedule, and writes telemetry into the
configured storage backends.

Uses meshcore-py's event-driven API:
  - commands.get_contacts() -> Event; event.payload is dict[pubkey_hex, contact]
  - commands.send_login_sync(contact, pwd) -> Event or None
  - commands.req_telemetry_sync(contact) -> list[{channel,type,value}] or None
  - commands.req_mma_sync(contact, start_secs_ago, end_secs_ago)
        -> list[{channel,type,min,max,avg}] or None

The MMA (min/max/avg) call returns ONE aggregate per channel for the entire
requested window - not per-hour samples - so for high-resolution history we
just poll live frequently and let the storage backend keep the timeseries.
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
    mc = await MeshCore.create_serial(cfg.port, baudrate=cfg.baud)
    if mc is None:
        raise SystemExit(
            f"Could not connect to gateway on {cfg.port}. "
            "Is it a serial companion-radio node?"
        )
    return mc


@dataclass
class SensorState:
    cfg: _config.SensorConfig
    next_poll_at: float = 0.0
    consecutive_failures: int = 0
    logged_in: bool = False


async def _resolve_contact(mc, pubkey_prefix: str) -> dict:
    """Look up a contact by pubkey prefix.

    meshcore-py's get_contacts() returns an Event whose payload is a dict
    keyed by full hex pubkey. Each value is a dict of contact attributes
    including 'public_key' and 'adv_name'.
    """
    event = await mc.commands.get_contacts()
    contacts = event.payload if event else None
    if not isinstance(contacts, dict):
        raise RuntimeError(f"Unexpected get_contacts payload: {type(contacts).__name__}")

    prefix = pubkey_prefix.lower()
    for pubkey_hex, contact in contacts.items():
        if pubkey_hex.lower().startswith(prefix):
            return contact
    raise LookupError(
        f"No contact matches pubkey prefix {pubkey_prefix!r} "
        f"(have {len(contacts)} contacts; first few: "
        f"{list(contacts.keys())[:3]})"
    )


async def _login_if_needed(mc, contact: dict, secret: str) -> bool:
    if not secret:
        return True
    result = await mc.commands.send_login_sync(contact, secret)
    if result is None:
        log.warning("[%s] login failed (no response / error)", contact.get("adv_name", "?"))
        return False
    return True


def _records_from_lpp(sensor_name: str, ts: int, kind: str,
                      channels_wanted: List[int], decoded: list) -> List[Measurement]:
    """Convert a list of CayenneLPP-decoded readings into Measurement rows.

    Accepts both:
      - live frame items: {channel, type, value}
      - mma items:        {channel, type, min, max, avg}  (caller picks kind per field)
    """
    out: List[Measurement] = []
    for item in decoded or []:
        ch = item.get("channel")
        if ch is None or (channels_wanted and ch not in channels_wanted):
            continue
        val = item.get("value")
        if val is None:
            continue
        if isinstance(val, (list, tuple, dict)):
            # Multi-value reading (e.g. GPS). Skip - not relevant for paxcounter.
            continue
        try:
            out.append(Measurement(ts=ts, sensor=sensor_name, channel=int(ch),
                                   kind=kind, value=float(val)))
        except (TypeError, ValueError):
            continue
    return out


async def _poll_live(mc, contact: dict, sensor: _config.SensorConfig) -> List[Measurement]:
    lpp = await mc.commands.req_telemetry_sync(contact)
    if not lpp:
        return []
    ts = int(time.time())
    return _records_from_lpp(sensor.name, ts, "live", sensor.channels, lpp)


async def _poll_mma(mc, contact: dict, sensor: _config.SensorConfig) -> List[Measurement]:
    """Ask the node for min/max/avg over the last poll-interval window.

    Returns one row per (channel, kind) where kind is 'window_min',
    'window_max', or 'window_avg'. All three share the same ts (poll time).
    """
    if sensor.mma_window_seconds <= 0:
        return []
    start_secs_ago = sensor.mma_window_seconds
    end_secs_ago = 0
    mma_list = await mc.commands.req_mma_sync(contact, start_secs_ago, end_secs_ago)
    if not mma_list:
        return []
    ts = int(time.time())
    out: List[Measurement] = []
    for item in mma_list:
        ch = item.get("channel")
        if ch is None or (sensor.channels and ch not in sensor.channels):
            continue
        for field, kind in (("min", "window_min"), ("max", "window_max"), ("avg", "window_avg")):
            v = item.get(field)
            if v is None or isinstance(v, (list, tuple, dict)):
                continue
            try:
                out.append(Measurement(ts=ts, sensor=sensor.name, channel=int(ch),
                                       kind=kind, value=float(v)))
            except (TypeError, ValueError):
                continue
    return out


async def _poll_sensor(mc, state: SensorState, storage) -> int:
    sensor = state.cfg
    contact = await _resolve_contact(mc, sensor.pubkey)
    if not state.logged_in:
        state.logged_in = await _login_if_needed(mc, contact, sensor.secret)
        if not state.logged_in and sensor.secret:
            # Login failed - skip this poll; will retry next interval
            raise RuntimeError("login required but failed")

    measurements: List[Measurement] = []
    measurements.extend(await _poll_live(mc, contact, sensor))
    measurements.extend(await _poll_mma(mc, contact, sensor))

    if storage and measurements:
        n = storage.write(measurements)
        log.info("[%s] wrote %d new rows (of %d collected)", sensor.name, n, len(measurements))
        return n
    elif measurements:
        log.info("[%s] dry-run: %d rows (storage disabled)", sensor.name, len(measurements))
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
                    await _poll_sensor(mc, st, storage)
                    st.consecutive_failures = 0
                except Exception:
                    st.consecutive_failures += 1
                    # Reset login state on any failure - cheap to re-login.
                    st.logged_in = False
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

# meshcore-trail-counter-logger

A small Python service that polls a [MeshCore](https://github.com/meshcore-dev/MeshCore)
paxcounter node and records its Wi-Fi / BLE PAX counts into a time-series
database for long-term trail-usage analysis.

Built to work with the `examples/paxcounter` firmware in MeshCore.

## What it does

On a schedule (default: every 5 minutes), the logger:

1. Connects to a local MeshCore *gateway* node over USB-serial
2. Logs in to the configured paxcounter contact
3. Asks for the **live** telemetry (channels 2/3/4/5 — current hour Wi-Fi/BLE
   in-progress count + 24 h rolling sum)
4. Asks for the **historical** hourly samples for the past N days (default 7)
   via `GET_AVG_MIN_MAX`, so any gaps from logger / mesh downtime get
   backfilled automatically
5. Inserts the rows into SQLite and/or InfluxDB (both are supported,
   independently enabled via config)

A bundled `docker-compose.yml` brings up Grafana + InfluxDB with a starter
dashboard for quick visualization.

## Hardware required

- A "paxcounter" MeshCore node deployed on the trail
  (see [MeshCore `examples/paxcounter`](https://github.com/meshcore-dev/MeshCore/tree/master/examples/paxcounter))
- A "gateway" MeshCore companion-radio node connected via USB to the machine
  running this logger (any Heltec V3 / RAK / T-Deck etc. flashed with a
  standard companion-radio build)
- A small always-on host: Raspberry Pi Zero 2 W is plenty

## Quick start

```bash
git clone https://github.com/cbattlegear/meshcore-trail-counter-logger.git
cd meshcore-trail-counter-logger
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp config.example.toml config.toml
# edit config.toml - set gateway serial port, paxcounter pubkey, login secret
python -m trail_logger.poller
```

For the Grafana + InfluxDB stack:

```bash
docker compose up -d
# then visit http://localhost:3000  (admin / admin on first login)
```

## Configuration

See `config.example.toml` for the full set of options. The essentials:

```toml
[gateway]
transport = "serial"
port = "/dev/ttyUSB0"        # COM3 on Windows
baud = 115200

[[sensors]]
name = "trailhead-north"
pubkey = "abc123..."           # paxcounter node's pubkey (first 6+ hex chars)
secret = "loginpasswordhere"   # login secret if the node requires one
poll_interval_seconds = 300
backfill_hours = 168           # request 7 days of history each poll (dedupes)

[storage.sqlite]
enabled = true
path = "data/trail.db"

[storage.influxdb]
enabled = false
url = "http://localhost:8086"
token = "..."
org = "trail"
bucket = "paxcounter"
```

## Schema (SQLite)

```sql
CREATE TABLE measurements (
    ts          INTEGER NOT NULL,   -- unix epoch seconds (UTC)
    sensor      TEXT NOT NULL,      -- sensor name from config
    channel     INTEGER NOT NULL,   -- CayenneLPP channel id (1=batt, 2=wifi, 3=ble, ...)
    kind        TEXT NOT NULL,      -- 'live' | 'hourly_avg' | 'hourly_min' | 'hourly_max'
    value       REAL NOT NULL,
    PRIMARY KEY (ts, sensor, channel, kind)
);
CREATE INDEX idx_measurements_sensor_ts ON measurements (sensor, ts);
```

The `PRIMARY KEY` makes inserts idempotent — re-polling overlapping history
just no-ops.

## Caveats

- **Counts are an index, not a headcount.** Modern phones randomize MACs;
  paxcounter sees more "devices" than there are people. Compare like-for-like
  windows (week-over-week, month-over-month) rather than reading raw numbers
  as occupancy.
- **Time-sync matters.** Make sure both your paxcounter and gateway have
  reasonable clocks, or hourly buckets will smear.
- This project assumes a single gateway. Multi-gateway support is on the
  roadmap.

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgments

- [MeshCore](https://github.com/meshcore-dev/MeshCore) by Scott Powell & contributors
- [`meshcore-py`](https://github.com/meshcore-dev/meshcore_py) — the Python client library this depends on
- [libpax](https://github.com/dbinfrago/libpax) — the actual PAX-counting library used by the firmware
- Originally inspired by Meshtastic's PaxcounterModule

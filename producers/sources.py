"""StreamBharat source systems, as replayed into Kafka by producers/replay.py.

    source        topic                        key         from
    player        ott.player.events            user_id     player/playback_YYYY-MM-DD.json (sent at server_ts)
    subscriptions ott.billing.subscriptions    user_id     billing/subscription_events.json
    settlements   ott.billing.settlements      user_id     billing/gateway_settlements.csv
    search        ott.app.search               profile_id  search/search_logs.json
    ratings       ott.app.ratings              profile_id  app/ratings.json
    ad_breaks     ott.ads.breaks               user_id     ads/ad_breaks.csv
    impressions   ott.ads.impressions          user_id     ads/ad_impressions.csv

Examples (inside the workspace container: docker compose exec workspace bash):
    python producers/replay.py                                    # October 2026, 1 day per minute
    python producers/replay.py --from 2026-10-01 --to 2026-10-02 --speed 0
    python producers/replay.py --sources player,ad_breaks,impressions --speed 360
    python producers/replay.py --dry-run --speed 0                # no Kafka: writes _dryrun/<topic>.jsonl
"""
import csv, json, os, zlib
from datetime import datetime, timedelta

from replay_engine import Source, parse_ts

PROJECT = "streambharat"
DEFAULT_FROM = "2026-10-01"
DEFAULT_TO = "2026-10-31"
DOC = __doc__

PLAYER_V2_FROM = datetime(2026, 10, 15)      # Android 7.3.0 rollout: position in milliseconds, network type added


def read_player(ctx):
    """Player files are named by the day events reached the server; send each event at its server_ts."""
    for d in ctx.days():
        p = f"{ctx.data}/player/playback_{d.isoformat()}.json"
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            rows = [json.loads(l) for l in f if l.strip()]
        rows.sort(key=lambda r: (r["server_ts"], r["event_id"]))
        for r in rows:
            yield parse_ts(r["server_ts"]), r["user_id"], r


def player_version(ts, rec):
    if ts < PLAYER_V2_FROM or not (rec.get("device_type") == "android" and rec.get("app_version", "").startswith("7.3")):
        return rec, "1"
    r = dict(rec)
    r["position_ms"] = int(r.pop("position_sec", 0) or 0) * 1000
    r["network"] = "wifi" if zlib.crc32(rec["device_id"].encode()) % 3 else "cellular"
    return r, "2"


def _json_rows(path, ts_col, key_col, ctx):
    prefix = (f'"{ts_col}": "{ctx.d_from:%Y-%m}', f'"{ts_col}": "{ctx.d_to:%Y-%m}')
    rows = []
    with open(path, encoding="utf-8") as f:
        for l in f:
            if prefix[0] not in l and prefix[1] not in l:
                continue
            r = json.loads(l)
            ts = parse_ts(r[ts_col])
            if ctx.in_range(ts):
                rows.append((ts, r[key_col], r))
    rows.sort(key=lambda x: x[0])
    return rows


def _csv_rows(path, ts_col, key_col, ctx):
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            ts = parse_ts(r[ts_col])
            if ctx.in_range(ts):
                rows.append((ts, r[key_col], r))
    rows.sort(key=lambda x: x[0])
    return rows


def read_subs(ctx):
    yield from _json_rows(f"{ctx.data}/billing/subscription_events.json", "event_ts", "user_id", ctx)


def read_settlements(ctx):
    yield from _csv_rows(f"{ctx.data}/billing/gateway_settlements.csv", "settled_ts", "user_id", ctx)


def read_search(ctx):
    yield from _json_rows(f"{ctx.data}/search/search_logs.json", "ts", "profile_id", ctx)


def read_ratings(ctx):
    yield from _json_rows(f"{ctx.data}/app/ratings.json", "ts", "profile_id", ctx)


def read_breaks(ctx):
    yield from _csv_rows(f"{ctx.data}/ads/ad_breaks.csv", "break_ts", "user_id", ctx)


def read_impressions(ctx):
    # the ad server reports an impression when the ad finishes (or is skipped)
    for ts, key, r in _csv_rows(f"{ctx.data}/ads/ad_impressions.csv", "ad_break_ts", "user_id", ctx):
        yield ts + timedelta(seconds=int(r["duration_sec"] or 0)), key, r


SOURCES = [
    Source("player", "ott.player.events", read_player, "player-sdk", late_p=0.003, dup_p=0.004, dup_seconds=(1, 60),
           version=player_version, ts_field="client_ts", key_fields=("user_id", "profile_id")),
    Source("subscriptions", "ott.billing.subscriptions", read_subs, "billing", late_p=0.01, dup_p=0.01,
           amount_field="price", ts_field="event_ts", key_fields=("user_id",)),
    Source("settlements", "ott.billing.settlements", read_settlements, "payment-gateway", late_p=0.02,
           late_minutes=(30, 360), dup_p=0.005, amount_field="amount_inr", ts_field="settled_ts", key_fields=("user_id",)),
    Source("search", "ott.app.search", read_search, "search-api", late_p=0.005, dup_p=0.003, ts_field="ts",
           key_fields=("profile_id",)),
    Source("ratings", "ott.app.ratings", read_ratings, "app-api", late_p=0.005, dup_p=0.003, ts_field="ts",
           key_fields=("profile_id",)),
    Source("ad_breaks", "ott.ads.breaks", read_breaks, "ad-decision-server", late_p=0.005, dup_p=0.003,
           ts_field="break_ts", key_fields=("user_id",)),
    Source("impressions", "ott.ads.impressions", read_impressions, "ad-tracking", late_p=0.02, late_minutes=(2, 30),
           dup_p=0.008, dup_seconds=(1, 20), amount_field="cpm_inr", ts_field="ad_break_ts", key_fields=("user_id", "campaign_id")),
]

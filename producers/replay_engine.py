"""Replay engine shared by the source-system replayers. You should not need to change this file.

producers/sources.py describes the source systems (which files, which topic, which key, how messy).
This engine merges them in time order, plays them on a simulated clock and sends them to Kafka,
adding the imperfections real upstream systems have: duplicates, late messages, broken messages
(re-sent correctly later) and a schema change part-way through.
"""
import argparse, heapq, itertools, json, os, random, sys, time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Optional


@dataclass
class Source:
    name: str                                   # short name used with --sources
    topic: str
    reader: Callable                            # reader(ctx) -> iterable of (send_time, key, value[, headers])
    source_system: str
    kind: str = "json"                          # "json": value is a dict; "text": value is a str (CSV line, HL7 ...)
    late_p: float = 0.01                        # share of messages held back
    late_minutes: tuple = (5, 45)
    dup_p: float = 0.003                        # share of messages sent twice (upstream retry)
    dup_seconds: tuple = (1, 300)
    poison_p: float = 0.0004                    # share of messages broken (and re-sent correctly later)
    version: Optional[Callable] = None          # version(send_time, value) -> (value, "1"/"2")
    amount_field: Optional[str] = None          # field to corrupt with a thousands comma
    ts_field: Optional[str] = None              # field to corrupt with slashes in the date
    key_fields: tuple = ()                      # fields removed by the "missing key" break
    sorted_input: bool = True                   # reader yields in time order (else the engine sorts)


@dataclass
class Context:
    data: str
    d_from: date
    d_to: date
    rnd: random.Random = field(default_factory=random.Random)

    def days(self):
        d = self.d_from
        while d <= self.d_to:
            yield d
            d += timedelta(days=1)

    def in_range(self, dt):
        return self.d_from <= dt.date() <= self.d_to


def parse_ts(s):
    """'2026-10-05 10:00:00', '2026-10-05T10:00:00Z', '2026-10-05T10:00:00.123+05:30' -> naive datetime."""
    s = s.replace("T", " ")
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")


# ---------------- sinks ----------------
class DryRunSink:
    def __init__(self):
        os.makedirs("_dryrun", exist_ok=True)
        self.files = {}

    def send(self, topic, key, value, headers):
        f = self.files.get(topic)
        if f is None:
            f = self.files[topic] = open(f"_dryrun/{topic}.jsonl", "w", encoding="utf-8")
        f.write(json.dumps({"key": key, "headers": {k: v.decode() for k, v in headers},
                            "value": value.decode(errors="replace")}) + "\n")

    def poll(self):
        pass

    def close(self):
        for f in self.files.values():
            f.close()
        return 0


class KafkaSink:
    def __init__(self, bootstrap, client_id):
        from confluent_kafka import Producer
        self.errors = 0
        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "enable.idempotence": True,       # no duplicates from the producer's own retries
            "acks": "all",
            "linger.ms": 20,
            "batch.size": 131072,
            "compression.type": "lz4",
        })

    def _cb(self, err, msg):
        if err is not None:
            self.errors += 1
            if self.errors <= 5:
                print("delivery failed:", err, file=sys.stderr)

    def send(self, topic, key, value, headers):
        while True:
            try:
                self.p.produce(topic, key=key.encode() if key else None, value=value, headers=headers, on_delivery=self._cb)
                break
            except BufferError:
                self.p.poll(0.5)              # local queue full: wait for deliveries
        self.p.poll(0)

    def poll(self):
        self.p.poll(0)

    def close(self):
        left = self.p.flush(30)
        return self.errors + left


# ---------------- the run ----------------
def main(spec):
    """spec: the producers/sources.py module (PROJECT, DEFAULT_FROM, DEFAULT_TO, SOURCES, DOC)."""
    sources_all = {s.name: s for s in spec.SOURCES}
    ap = argparse.ArgumentParser(description=spec.DOC, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=os.environ.get("RAW_DATA", "data/raw"), help="folder with the raw dataset")
    ap.add_argument("--from", dest="date_from", default=spec.DEFAULT_FROM)
    ap.add_argument("--to", dest="date_to", default=spec.DEFAULT_TO, help="inclusive")
    ap.add_argument("--sources", default=",".join(sources_all))
    ap.add_argument("--speed", type=float, default=1440,
                    help="simulated seconds per real second (1440 = one day per minute; 0 = no waiting)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-chaos", action="store_true", help="send a clean feed (no duplicates, late or broken messages)")
    ap.add_argument("--dry-run", action="store_true", help="do not connect to Kafka; write messages to _dryrun/")
    args = ap.parse_args()

    names = [s.strip() for s in args.sources.split(",") if s.strip()]
    for n in names:
        if n not in sources_all:
            sys.exit(f"unknown source {n!r}; choose from {', '.join(sources_all)}")
    sources = [sources_all[n] for n in names]
    ctx = Context(args.data, date.fromisoformat(args.date_from), date.fromisoformat(args.date_to), random.Random(args.seed))
    if not os.path.isdir(args.data):
        sys.exit(f"dataset folder {args.data!r} not found: copy the dataset's data/raw folder into the project")
    rnd = random.Random(args.seed)
    chaos = not args.no_chaos
    sink = DryRunSink() if args.dry_run else KafkaSink(args.bootstrap, f"{spec.PROJECT}-replay")

    def tagged(src):
        rows = src.reader(ctx)
        if not src.sorted_input:
            rows = sorted(rows, key=lambda r: r[0])
        for row in rows:
            ts, key, value = row[0], row[1], row[2]
            extra = row[3] if len(row) > 3 else None
            yield ts, src.name, key, value, extra

    def poison(src, value):
        """A broken message body, the way real producers break them."""
        if src.kind == "text":
            kind = rnd.choice(["truncated", "garbage"])
            if kind == "truncated":
                return value[: rnd.randint(5, max(6, len(value) // 2))].encode()
            return f"\x00\x00ERR|{src.name}|CONNECTION_RESET".encode()
        kinds = ["truncated", "not_json"]
        if src.amount_field:
            kinds.append("bad_amount")
        if src.ts_field:
            kinds.append("bad_ts")
        if src.key_fields:
            kinds.append("missing_key")
        kind = rnd.choice(kinds)
        r = dict(value)
        if kind == "truncated":
            s = json.dumps(r)
            return s[: rnd.randint(10, max(11, len(s) - 5))].encode()
        if kind == "not_json":
            return f"ERR|{src.name}|{rnd.randint(1, 10**9)}|RETRY_LATER".encode()
        if kind == "bad_amount":
            try:
                r[src.amount_field] = f"{float(r[src.amount_field]):,.2f}"      # "1,23,456.00"-style text
            except (TypeError, ValueError, KeyError):
                r[src.amount_field] = "NaN"
        elif kind == "bad_ts" and r.get(src.ts_field):
            r[src.ts_field] = str(r[src.ts_field]).replace("-", "/", 2)     # 2026/10/05 10:00:00
        elif kind == "missing_key":
            for k in src.key_fields:
                r.pop(k, None)
        return json.dumps(r).encode()

    stats = {s.name: {"sent": 0, "dup": 0, "late": 0, "poison": 0, "v2": 0} for s in sources}
    by_name = {s.name: s for s in sources}

    def emit(item, sim_now):
        ts, name, key, value, extra, flag = item
        src = by_name[name]
        version = "1"
        if src.version:
            value, version = src.version(ts, value)
        if flag == "poison":
            body = poison(src, value)
        elif src.kind == "text":
            body = value.encode()
        else:
            body = json.dumps(value).encode()
        headers = [("source_system", src.source_system.encode()), ("schema_version", version.encode()),
                   ("sent_at", sim_now.strftime("%Y-%m-%dT%H:%M:%S").encode())]
        for k, v in (extra or {}).items():
            headers.append((k, str(v).encode()))
        sink.send(src.topic, key, body, headers)
        st = stats[name]
        st["sent"] += 1
        st["v2"] += version == "2"

    sim0 = datetime.combine(ctx.d_from, datetime.min.time())
    real0 = time.monotonic()
    last = [time.monotonic()]

    def wait_until(sim_t):
        if args.speed <= 0:
            return
        target = real0 + (sim_t - sim0).total_seconds() / args.speed
        delay = target - time.monotonic()
        while delay > 0:
            time.sleep(min(delay, 0.5))
            sink.poll()
            delay = target - time.monotonic()

    def report(sim_now, force=False):
        if force or time.monotonic() - last[0] >= 10:
            last[0] = time.monotonic()
            parts = ", ".join(f"{s}={v['sent']}" for s, v in stats.items())
            print(f"[sim {sim_now:%Y-%m-%d %H:%M}] sent: {parts}", flush=True)

    print(f"replaying {', '.join(names)} from {ctx.d_from} to {ctx.d_to} at speed {args.speed:g}"
          f" -> {'_dryrun/' if args.dry_run else args.bootstrap}  (chaos {'on' if chaos else 'off'})", flush=True)
    stream = heapq.merge(*(tagged(s) for s in sources), key=lambda x: x[0])
    pending = []
    seq = itertools.count()
    sim_now = sim0
    try:
        for ts, name, key, value, extra in stream:
            while pending and pending[0][0] <= ts:
                rel, _, item = heapq.heappop(pending)
                wait_until(rel)
                emit(item, rel)
            wait_until(ts)
            sim_now = ts
            src = by_name[name]
            flag = None
            if chaos:
                if rnd.random() < src.poison_p:
                    flag = "poison"
                    stats[name]["poison"] += 1
                    # the upstream team notices and re-sends the correct message 10-90 minutes later
                    heapq.heappush(pending, (ts + timedelta(minutes=rnd.randint(10, 90)), next(seq),
                                             (ts, name, key, value, extra, None)))
                elif rnd.random() < src.late_p:
                    lo, hi = src.late_minutes
                    heapq.heappush(pending, (ts + timedelta(minutes=rnd.randint(lo, hi)), next(seq),
                                             (ts, name, key, value, extra, None)))
                    stats[name]["late"] += 1
                    continue
            emit((ts, name, key, value, extra, flag), ts)
            if chaos and flag is None and rnd.random() < src.dup_p:
                lo, hi = src.dup_seconds
                heapq.heappush(pending, (ts + timedelta(seconds=rnd.randint(lo, hi)), next(seq),
                                         (ts, name, key, value, extra, None)))
                stats[name]["dup"] += 1
            report(sim_now)
        while pending:
            rel, _, item = heapq.heappop(pending)
            wait_until(rel)
            emit(item, rel)
    except KeyboardInterrupt:
        print("stopping (Ctrl+C)")
    failed = sink.close()
    report(sim_now, force=True)
    print("summary:")
    for s in sources:
        v = stats[s.name]
        print(f"  {s.topic:<30} sent {v['sent']:>7}  duplicates {v['dup']:>4}  late {v['late']:>4}  broken {v['poison']:>3}"
              + (f"  schema v2 {v['v2']}" if s.version else ""))
    if failed:
        print(f"WARNING: {failed} messages were not delivered", file=sys.stderr)
        sys.exit(1)

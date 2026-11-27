"""StreamBharat live stream simulator for Q14 (trending now) and Q15 (ad pacing and frequency capping).

Run inside the workspace container while your Spark stream runs:
    python producers/live.py --stream plays     # Q14
    python producers/live.py --stream ads       # Q15
    python producers/live.py --stream both --seconds 900

Every 2 seconds it sends a batch of events to Kafka:
    ott.live.plays    event_id, user_id, profile_id, content_id, language, event_ts       (one row per playback start)
    ott.live.ads      impression_id, user_id, campaign_id, ad_break_ts, duration_sec, cpm_inr

Titles come from content/catalog_2026-10-01.json and campaigns from ads/ad_campaigns.csv.
The streams contain realistic surprises for you to detect.
"""
import argparse, csv, json, os, random, time
from collections import Counter
from datetime import datetime, timedelta
from live_sink import add_args, make_write, now_ist

ap = argparse.ArgumentParser()
ap.add_argument("--stream", choices=["plays", "ads", "both"], default="both")
ap.add_argument("--seconds", type=int, default=0)
ap.add_argument("--seed", type=int, default=9)
add_args(ap)
args = ap.parse_args()
rnd = random.Random(args.seed)

cat = json.load(open("data/raw/content/catalog_2026-10-01.json", encoding="utf-8"))
titles = []
for t in cat:
    if t["type"] in ("movie", "series"):
        titles.append((t["content_id"], t["languages"][0], rnd.paretovariate(1.3)))
weights = [w for _, _, w in titles]
camps = list(csv.DictReader(open("data/raw/ads/ad_campaigns.csv", encoding="utf-8")))
users = [f"U{i:06d}" for i in range(1, 2501)]

surge_pool = rnd.sample(titles, 6)          # titles that suddenly start trending, one after another
overspender = rnd.choice(camps)["campaign_id"]
sticky_users = rnd.sample(users, 15)          # these users keep getting the same ad


TOPICS = {'plays': 'ott.live.plays', 'ads': 'ott.live.ads'}
KEYS = {'plays': 'user_id', 'ads': 'user_id'}
write = make_write(args, TOPICS, KEYS, "streambharat-live")   # write(feed, rows): sends rows to the feed's topic


ev = imp = 0
start = time.time()
print(f"sending to {'_dryrun/' if args.dry_run else args.bootstrap} ... Ctrl+C to stop", flush=True)
try:
    while not args.seconds or time.time() - start < args.seconds:
        now = now_ist()
        stamp = now.strftime("%Y%m%d_%H%M%S")
        el = time.time() - start
        surge = surge_pool[int(el // 180) % len(surge_pool)]          # a new title surges every 3 minutes
        surge_strength = min(1.0, (el % 180) / 60)                    # ramps up over the first minute
        plays, ads = [], []
        if args.stream in ("plays", "both"):
            for _ in range(rnd.randint(60, 100)):
                ev += 1
                t = surge if rnd.random() < 0.18 * surge_strength else rnd.choices(titles, weights=weights)[0]
                u = rnd.choice(users)
                ts = now - timedelta(seconds=rnd.randint(0, 4))
                if rnd.random() < 0.03:
                    ts -= timedelta(minutes=rnd.randint(2, 12))         # late events
                plays.append({"event_id": f"LP{ev:09d}", "user_id": u, "profile_id": f"{u}-P1", "content_id": t[0], "language": t[1],
                              "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S")})
        if args.stream in ("ads", "both"):
            for _ in range(rnd.randint(40, 70)):
                imp += 1
                u = rnd.choice(users)
                c = rnd.choice(camps)
                if rnd.random() < 0.25:
                    c = next(x for x in camps if x["campaign_id"] == overspender)   # one campaign spends far too fast
                if rnd.random() < 0.08:
                    u = rnd.choice(sticky_users)
                    c = camps[sum(map(ord, u)) % len(camps)]               # always the same campaign for this user
                ads.append({"impression_id": f"LI{imp:09d}", "user_id": u, "campaign_id": c["campaign_id"],
                            "ad_break_ts": (now - timedelta(seconds=rnd.randint(0, 4))).strftime("%Y-%m-%dT%H:%M:%S"),
                            "duration_sec": rnd.choice([10, 15, 20, 30]), "cpm_inr": int(c["cpm_inr"])})
        write("plays", plays, stamp)
        write("ads", ads, stamp)
        time.sleep(2)
except KeyboardInterrupt:
    pass
print("stopped")

"""Checks that the whole stack works: Kafka, Spark, Delta Lake and Postgres.

    docker compose exec workspace python -m sbetl.smoke

This is a connectivity check only. It is not a solution to any question.
"""
import sys
import time

from confluent_kafka.admin import AdminClient

from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def step(name, fn):
    t = time.time()
    try:
        detail = fn()
        print(f"  OK    {name:<34} {detail or ''}  ({time.time() - t:.1f}s)")
        return True
    except Exception as e:                       # report and carry on with the other checks
        print(f"  FAIL  {name:<34} {type(e).__name__}: {str(e).splitlines()[0][:150]}")
        return False


def main():
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap"]
    print(f"StreamBharat stack check (Kafka at {bootstrap})")
    results = []

    def kafka_topics():
        md = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
        mine = sorted(t for t in md.topics if t.startswith("ott."))
        missing = set(cfg["kafka"]["topics"].values()) - set(mine)
        if missing:
            raise RuntimeError(f"missing topics {sorted(missing)}: run 'docker compose up kafka-init'")
        return f"{len(mine)} ott.* topics"
    results.append(step("Kafka reachable, topics exist", kafka_topics))

    spark = get_spark("smoke_test")
    results.append(step("Spark session", lambda: f"Spark {spark.version}, master {spark.sparkContext.master}"))

    def kafka_from_spark():
        n = (spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap)
             .option("subscribe", cfg["kafka"]["topics"]["player"]).option("startingOffsets", "earliest")
             .load().count())
        return f"{n} messages in {cfg['kafka']['topics']['player']}" + ("  (run producers/replay.py to fill it)" if n == 0 else "")
    results.append(step("Spark reads Kafka", kafka_from_spark))

    def delta_roundtrip():
        path = f"{cfg['paths']['lake']}/_smoke/delta_check"
        spark.range(5).write.format("delta").mode("overwrite").save(path)
        n = spark.read.format("delta").load(path).count()
        assert n == 5, n
        return path
    results.append(step("Delta Lake write and read", delta_roundtrip))

    def raw_data():
        n = spark.read.option("header", True).csv(f"{cfg['paths']['raw']}/reference/plans.csv").count()
        return f"{n} plans in data/raw/reference/plans.csv"
    results.append(step("StreamBharat dataset mounted", raw_data))

    def postgres():
        w = cfg["warehouse"]
        props = {"user": w["user"], "password": w["password"], "driver": "org.postgresql.Driver"}
        spark.createDataFrame([(1, "ok")], "id int, status string").write.jdbc(w["url"], "public._smoke", "overwrite", props)
        n = spark.read.jdbc(w["url"], "public._smoke", properties=props).count()
        return f"wrote and read {n} row in ottdw"
    results.append(step("Postgres warehouse (JDBC)", postgres))

    spark.stop()
    ok = all(results)
    print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED - see README 'Troubleshooting'")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

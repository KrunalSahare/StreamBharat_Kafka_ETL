"""Kafka topics -> bronze Delta tables, with a dead-letter path for broken messages.

Question(s): Q3, Q4 in the workbook.

What this job must do:
  - One streaming query per source topic, raw value + Kafka metadata.
  - Q4: unparseable messages to ott.dlq and a Delta table.

Run it (inside the workspace container):
    python -m sbetl.jobs.bronze_ingest
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("bronze_ingest")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q3, Q4 in the workbook")


if __name__ == "__main__":
    main()

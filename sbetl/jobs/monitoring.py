"""Streaming query metrics and lag -> a monitoring table.

Question(s): Q18 in the workbook.

What this job must do:
  - lastProgress or a StreamingQueryListener.
  - Lag per topic and partition.

Run it (inside the workspace container):
    python -m sbetl.jobs.monitoring
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("monitoring")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q18 in the workbook")


if __name__ == "__main__":
    main()

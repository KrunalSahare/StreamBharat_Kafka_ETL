"""Reusable data quality rules, quarantine and a quality metrics table.

Question(s): Q11 in the workbook.

What this job must do:
  - Rules as data.
  - Metrics per rule per batch.

Run it (inside the workspace container):
    python -m sbetl.jobs.quality
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("quality")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q11 in the workbook")


if __name__ == "__main__":
    main()

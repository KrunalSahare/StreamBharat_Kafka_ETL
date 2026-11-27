"""gold.watch_time_daily, gold.dau_mau, gold.top_titles.

Question(s): Q12 in the workbook.

What this job must do:
  - Episodes rolled up to series.
  - Compare with published_metrics.csv.

Run it (inside the workspace container):
    python -m sbetl.jobs.gold_watch_time
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("gold_watch_time")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q12 in the workbook")


if __name__ == "__main__":
    main()

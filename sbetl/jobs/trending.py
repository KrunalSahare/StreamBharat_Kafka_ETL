"""Live trending titles from ott.live.plays -> ott.metrics.trending.

Question(s): Q14 in the workbook.

What this job must do:
  - Sliding windows per language.
  - Spot the surge within a minute.

Run it (inside the workspace container):
    python -m sbetl.jobs.trending
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("trending")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q14 in the workbook")


if __name__ == "__main__":
    main()

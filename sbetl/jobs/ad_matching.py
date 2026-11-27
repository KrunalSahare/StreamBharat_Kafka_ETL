"""Match ad breaks with impressions; fill rate and frequency caps.

Question(s): Q10 in the workbook.

What this job must do:
  - Stream-stream join with watermarks.
  - Unfilled breaks and orphan impressions.

Run it (inside the workspace container):
    python -m sbetl.jobs.ad_matching
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("ad_matching")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q10 in the workbook")


if __name__ == "__main__":
    main()

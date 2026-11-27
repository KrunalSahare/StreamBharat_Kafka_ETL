"""Ad pacing and frequency-cap alerts from ott.live.ads -> ott.alerts.ad_pacing.

Question(s): Q15 in the workbook.

What this job must do:
  - Spend rate per campaign.
  - Impressions per user per campaign.

Run it (inside the workspace container):
    python -m sbetl.jobs.ad_pacing
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("ad_pacing")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q15 in the workbook")


if __name__ == "__main__":
    main()

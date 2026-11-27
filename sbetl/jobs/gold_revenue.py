"""gold.revenue_reconciliation and gold.royalties.

Question(s): Q13 in the workbook.

What this job must do:
  - Charged vs settled.
  - Partner valid at viewing time.

Run it (inside the workspace container):
    python -m sbetl.jobs.gold_revenue
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("gold_revenue")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q13 in the workbook")


if __name__ == "__main__":
    main()

"""Subscription events -> silver.subscriptions (SCD2) and the compacted state topic.

Question(s): Q8 in the workbook.

What this job must do:
  - State machine per user.
  - Payment failures that recover are not churn.

Run it (inside the workspace container):
    python -m sbetl.jobs.subscription_state
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("subscription_state")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q8 in the workbook")


if __name__ == "__main__":
    main()

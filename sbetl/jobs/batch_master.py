"""Catalog snapshots, licences, users, profiles and reference data -> silver (batch).

Question(s): Q6 in the workbook.

What this job must do:
  - Flatten series -> seasons -> episodes; standardise genres.
  - Make the job safe to rerun.

Run it (inside the workspace container):
    python -m sbetl.jobs.batch_master
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("batch_master")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q6 in the workbook")


if __name__ == "__main__":
    main()

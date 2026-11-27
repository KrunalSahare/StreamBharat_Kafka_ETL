"""Load gold tables into Postgres (ottdw) for dashboards.

Question(s): Q16 in the workbook.

What this job must do:
  - Staging table + upsert.
  - Indexes and views for Metabase.

Run it (inside the workspace container):
    python -m sbetl.jobs.serve_postgres
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("serve_postgres")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q16 in the workbook")


if __name__ == "__main__":
    main()

"""bronze player events -> silver.player_events: clocks corrected, de-duplicated, both schema versions.

Question(s): Q5 in the workbook.

What this job must do:
  - Device clock correction.
  - position_sec and position_ms.
  - Offline downloads arrive days late.

Run it (inside the workspace container):
    python -m sbetl.jobs.silver_player
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("silver_player")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q5 in the workbook")


if __name__ == "__main__":
    main()

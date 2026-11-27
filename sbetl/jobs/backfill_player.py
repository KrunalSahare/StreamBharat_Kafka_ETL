"""July - September player files + October from Kafka -> one silver.player_events.

Question(s): Q7 in the workbook.

What this job must do:
  - Load history in batch.
  - No gap, no overlap; 12 Sep held-back events.

Run it (inside the workspace container):
    python -m sbetl.jobs.backfill_player
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("backfill_player")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q7 in the workbook")


if __name__ == "__main__":
    main()

"""Player events -> viewing sessions and watch time in real time.

Question(s): Q9 in the workbook.

What this job must do:
  - Stateful sessions per device and title.
  - Cap runaway sessions; seeks are not watch time.

Run it (inside the workspace container):
    python -m sbetl.jobs.sessions
"""
from sbetl.common.config import load_config
from sbetl.common.spark import get_spark


def main():
    cfg = load_config()
    spark = get_spark("sessions")
    # TODO: your code here
    raise NotImplementedError("Not built yet: see Q9 in the workbook")


if __name__ == "__main__":
    main()

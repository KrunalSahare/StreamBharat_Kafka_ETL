"""Replays the StreamBharat source systems into Kafka. Options and examples: python producers/replay.py --help

The source systems (files, topics, keys) are described in producers/sources.py.
"""
import sources
from replay_engine import main

if __name__ == "__main__":
    main(sources)

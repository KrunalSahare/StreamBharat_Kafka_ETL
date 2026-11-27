"""Starter tests: the test setup works. Add your own tests for each job (Q20)."""
from sbetl.common.config import load_config


def test_config_has_all_topics():
    topics = load_config()["kafka"]["topics"]
    assert topics["player"] == "ott.player.events"
    assert len(topics) == 13


def test_spark_and_delta(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(3).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 3

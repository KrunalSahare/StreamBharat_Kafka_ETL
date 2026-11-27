#!/bin/sh
# Creates the StreamBharat topics. Safe to run again (--if-not-exists).
set -e
BS=kafka:9092
KT=/opt/kafka/bin/kafka-topics.sh

create() {   # name partitions [extra config]
  $KT --bootstrap-server $BS --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 $3
}

# Source systems (filled by producers/replay.py)
create ott.player.events              6 "--config retention.ms=1209600000"
create ott.billing.subscriptions      3
create ott.billing.settlements        3
create ott.app.search                 3
create ott.app.ratings                1
create ott.ads.breaks                 3
create ott.ads.impressions            3

# Live feeds (filled by producers/live.py)
create ott.live.plays                 6 "--config retention.ms=86400000"
create ott.live.ads                   3 "--config retention.ms=86400000"

# Written by YOUR Spark jobs
create ott.dlq                        1
create ott.metrics.trending           1
create ott.alerts.ad_pacing           1
create ott.ref.subscription_state     3 "--config cleanup.policy=compact"

echo "Topics:"
$KT --bootstrap-server $BS --list

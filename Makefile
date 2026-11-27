# Shortcuts. Windows without make: copy the command after each target into PowerShell.
export PROJECT_DIR := $(CURDIR)
EXEC = docker compose exec workspace

build:            ## build the Spark image (first time: 5-10 minutes)
	docker compose build
up:               ## core stack: Kafka, Kafka UI, Spark, Postgres, Jupyter
	docker compose up -d
up-all:           ## everything, including Airflow and Metabase
	docker compose --profile airflow --profile bi up -d
check:            ## smoke test: Kafka, Spark, Delta, Postgres
	$(EXEC) python -m sbetl.smoke
replay:           ## replay October 2026 into Kafka (1 simulated day per minute)
	$(EXEC) python producers/replay.py
replay-fast:      ## replay October 2026 as fast as possible
	$(EXEC) python producers/replay.py --speed 0
live:             ## live feeds: plays + ads (Ctrl+C to stop)
	$(EXEC) python producers/live.py --stream both
shell:            ## bash inside the workspace container
	$(EXEC) bash
test:             ## run pytest
	$(EXEC) pytest -q
topics:           ## list topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
lag:              ## consumer groups and their lag
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
psql:             ## SQL shell on the ottdw warehouse
	docker compose exec postgres psql -U ott -d ottdw
logs:
	docker compose logs -f --tail 100
down:             ## stop everything (Kafka data is lost; the lake and Postgres stay)
	docker compose --profile airflow --profile bi down
reset:            ## DANGER: wipe Kafka topics, the lake and checkpoints, then start fresh
	docker compose --profile airflow --profile bi down
	docker run --rm -v "$(CURDIR)/lake:/lake" alpine sh -c "rm -rf /lake/*"
	docker compose up -d

.PHONY: build up up-all check replay replay-fast live shell test topics lag psql logs down reset

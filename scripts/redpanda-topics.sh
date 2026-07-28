#!/usr/bin/env bash
# Create the two pipeline topics on the local Redpanda, idempotently.
#
# The pipeline has two seams, so two topics:
#   * blocks        — the producer's BlockUnit stream (producer -> transformer)
#   * decoded-rows  — the transformer's RowBatch stream (transformer -> writer)
#
# Partitioned by pool address (the producer sets the key), so every event for one
# pool stays ordered while different pools run in parallel. Retention is short —
# the log only needs to cover the reorg window plus consumer lag, and disk is a
# hard constraint.
#
# Run after `docker compose up -d redpanda`. Safe to run repeatedly: an existing
# topic is left untouched. The defaults match the [kafka] section in
# chainscope.toml / the CHAINSCOPE_KAFKA__* environment.
set -euo pipefail

BLOCKS_TOPIC="${CHAINSCOPE_KAFKA__BLOCKS_TOPIC:-chainscope.blocks}"
ROWS_TOPIC="${CHAINSCOPE_KAFKA__ROWS_TOPIC:-chainscope.decoded-rows}"
PARTITIONS="${CHAINSCOPE_KAFKA__PARTITIONS:-6}"
RETENTION_MS="${CHAINSCOPE_KAFKA__RETENTION_MS:-172800000}" # 48h
CONTAINER="${REDPANDA_CONTAINER:-chainscope-redpanda}"

rpk() { docker exec -i "$CONTAINER" rpk "$@"; }

ensure_topic() {
  local topic="$1"
  if rpk topic list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "$topic"; then
    echo "topic '$topic' already exists — leaving it"
  else
    echo "creating topic '$topic' (${PARTITIONS} partitions, retention.ms=${RETENTION_MS})"
    rpk topic create "$topic" \
      --partitions "$PARTITIONS" \
      --replicas 1 \
      --topic-config "retention.ms=${RETENTION_MS}"
  fi
}

ensure_topic "$BLOCKS_TOPIC"
ensure_topic "$ROWS_TOPIC"

echo "topics ready:"
rpk topic list

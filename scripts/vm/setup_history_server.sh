#!/usr/bin/env bash
# Install and start the Spark History Server as a systemd service on port 18080.
# Idempotent. Usage: sudo scripts/vm/setup_history_server.sh
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo)" >&2
  exit 1
fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p /data/spark-events
chown ubuntu:ubuntu /data/spark-events
install -m 0644 "$SCRIPT_DIR/spark-history.service" /etc/systemd/system/spark-history.service
systemctl daemon-reload
systemctl enable --now spark-history.service
systemctl restart spark-history.service

for _ in $(seq 1 30); do
  if curl -fs http://localhost:18080/api/v1/applications >/dev/null; then
    echo "History Server up: http://$(hostname -I | awk '{print $1}'):18080"
    exit 0
  fi
  sleep 2
done
echo "History Server did not answer on :18080; see journalctl -u spark-history" >&2
exit 1

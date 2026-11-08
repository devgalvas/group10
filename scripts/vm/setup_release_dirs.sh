#!/usr/bin/env bash
# One-time VM setup for prod deploys (AIS-38) and the dashboard service (AIS-39).
# Run as: sudo bash scripts/vm/setup_release_dirs.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_AS="${RUN_AS:-ubuntu}"

install -d -o "$RUN_AS" -g "$RUN_AS" /opt/ais-pipeline /opt/ais-pipeline/releases

install -m 0644 "$SCRIPT_DIR/ais-dashboard.service" /etc/systemd/system/ais-dashboard.service
systemctl daemon-reload
systemctl enable ais-dashboard.service
echo "release dirs ready; start the dashboard after the first deploy:"
echo "  sudo systemctl start ais-dashboard"

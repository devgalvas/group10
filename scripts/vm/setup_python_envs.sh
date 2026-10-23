#!/usr/bin/env bash
# Create the project venvs (~/ais-env-dev, ~/ais-env-prod) with Python 3.11 and print versions.
# Idempotent: existing venvs are reused. Usage: scripts/vm/setup_python_envs.sh [repo_dir]
# With repo_dir, the dev venv gets an editable install of that checkout.
set -euo pipefail

REPO_DIR="${1:-}"
PYTHON=python3.11
JAVA17=/usr/lib/jvm/java-17-openjdk-amd64

if [[ ! -x "$JAVA17/bin/java" ]]; then
  echo "Java 17 missing: sudo apt-get install -y openjdk-17-jdk-headless" >&2
  exit 1
fi

# The labs export SPARK_HOME=/opt/spark; pip's PySpark must use its own jars instead.
unset SPARK_HOME
export JAVA_HOME="$JAVA17"

for env in dev prod; do
  venv="$HOME/ais-env-$env"
  if [[ ! -x "$venv/bin/python" ]]; then
    echo "creating $venv"
    "$PYTHON" -m venv "$venv"
  fi
  "$venv/bin/pip" install -q --upgrade pip
done

if [[ -n "$REPO_DIR" ]]; then
  "$HOME/ais-env-dev/bin/pip" install -q -e "$REPO_DIR[dev]"
fi

for env in dev prod; do
  echo "== ais-env-$env"
  "$HOME/ais-env-$env/bin/python" - <<'PY'
import platform
try:
    import pyspark, importlib.metadata as md
    print("python", platform.python_version(), "| pyspark", pyspark.__version__,
          "| delta-spark", md.version("delta-spark"))
except ImportError:
    print("python", platform.python_version(), "| project not installed yet")
PY
done
"$JAVA_HOME/bin/java" -version 2>&1 | head -1

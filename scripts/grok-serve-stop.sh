#!/usr/bin/env bash
# Stop detached Grok serve started by grok-serve-detached.sh (or anything on the port).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${GROK_AGENT_PORT:-2419}"
PID_FILE="${ROOT}/target/grok-serve.pid"

stopped=0

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "stopping pid=$pid"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.15
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    stopped=1
  fi
  rm -f "$PID_FILE"
fi

# Also free the port if something else is listening (no pkill -f).
extra="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
if [[ -n "${extra}" ]]; then
  echo "freeing port $PORT: $extra"
  # shellcheck disable=SC2086
  kill $extra 2>/dev/null || true
  sleep 0.3
  # shellcheck disable=SC2086
  kill -9 $extra 2>/dev/null || true
  stopped=1
fi

if [[ "$stopped" -eq 1 ]]; then
  echo "stopped"
else
  echo "nothing to stop on port $PORT"
fi

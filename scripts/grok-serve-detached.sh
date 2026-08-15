#!/usr/bin/env bash
# Detached local Grok agent serve for Monoloop live examples.
# Does not block the caller; writes pid + log under target/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${GROK_AGENT_PORT:-2419}"
SECRET="${GROK_AGENT_SECRET:-monoloop-live-test}"
PID_FILE="${ROOT}/target/grok-serve.pid"
LOG_FILE="${ROOT}/target/grok-serve.log"
BIND="${GROK_AGENT_BIND:-127.0.0.1}"

mkdir -p "${ROOT}/target"

if [[ -f "$PID_FILE" ]]; then
  old="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old}" ]] && kill -0 "$old" 2>/dev/null; then
    echo "already running pid=$old (log: $LOG_FILE)"
    echo "  stop: $ROOT/scripts/grok-serve-stop.sh"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

if lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "error: port $PORT already in use" >&2
  lsof -iTCP:"$PORT" -sTCP:LISTEN >&2 || true
  exit 1
fi

if ! command -v grok >/dev/null 2>&1; then
  echo "error: grok not on PATH" >&2
  exit 1
fi

# Detach fully so agent/parent shells can exit without reaping the server.
nohup grok agent --always-approve serve \
  --bind "${BIND}:${PORT}" \
  --secret "$SECRET" \
  >"$LOG_FILE" 2>&1 &
pid=$!
echo "$pid" >"$PID_FILE"

# Bounded ready wait (never hang the caller forever).
ready=0
for _ in $(seq 1 40); do
  if lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "error: grok serve exited early; see $LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi
  sleep 0.25
done

if [[ "$ready" -ne 1 ]]; then
  echo "error: grok serve not listening on $PORT within 10s; see $LOG_FILE" >&2
  kill "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
  exit 1
fi

echo "grok serve detached"
echo "  pid:    $pid"
echo "  url:    ws://${BIND}:${PORT}/ws?server-key=${SECRET}"
echo "  log:    $LOG_FILE"
echo "  pidfile:$PID_FILE"
echo "  stop:   $ROOT/scripts/grok-serve-stop.sh"

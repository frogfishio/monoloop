#!/usr/bin/env bash
# Deterministic qualification for Interpreter assembly + chat projection.
# Optional: replay saved live dumps when present under target/.
#
# Usage (repo root):
#   ./scripts/qualify-interpreter-projection.sh
#   ./scripts/qualify-interpreter-projection.sh --with-replay-html
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WITH_REPLAY_HTML=0
for a in "$@"; do
  case "$a" in
    --with-replay-html) WITH_REPLAY_HTML=1 ;;
    -h|--help)
      sed -n '1,12p' "$0"
      exit 0
      ;;
  esac
done

echo "=== 1/4 monoloop-interpreter (sentence + ACP core) ==="
cargo test -p monoloop-interpreter --lib sentence -- --nocapture
cargo test -p monoloop-interpreter --test interpreter_core -- --nocapture

echo
echo "=== 2/4 monoloop-testkit chat_projector + html_report unit ==="
cargo test -p monoloop-testkit --lib chat_projector -- --nocapture
cargo test -p monoloop-testkit --lib html_report -- --nocapture

echo
echo "=== 3/4 qualification matrix (fixtures, no live Grok) ==="
cargo test -p monoloop-testkit --test qualification_projection -- --nocapture

echo
echo "=== 4/4 saved live dump replay (if any) ==="
REPLAYED=0
for dump in \
  target/live_grok_crud.raw.txt \
  target/live_grok_analyze.raw.txt \
  target/live_grok_ask.raw.txt
do
  if [[ -f "$dump" ]]; then
    echo "--- replay $dump ---"
    if [[ "$WITH_REPLAY_HTML" -eq 1 ]]; then
      cargo run -q -p monoloop-testkit --example replay_raw_html -- "$dump" || true
    else
      # Lightweight: matrix test already covers dump when present
      echo "(present; covered by q_replay_saved_live_dumps_if_present)"
    fi
    REPLAYED=1
  fi
done
if [[ "$REPLAYED" -eq 0 ]]; then
  echo "(no target/live_grok_*.raw.txt yet — capture later with live_grok_ask)"
fi

echo
echo "=== QUALIFICATION SUMMARY ==="
echo "Interpreter: sentence v2 + ACP fragmentation + tool lifecycle"
echo "Projection:  StructuralOrdinalZip | ChronologicalChat gates"
echo "HTML:        chat projection + event-order truth sections"
echo "Live dumps:  optional under target/ for deeper dissection"
echo
echo "All deterministic suites passed."
echo "To capture a new live sample (managed serve, waits for Grok):"
echo "  cargo run -p monoloop-testkit --example live_grok_ask -- --preset analyze"

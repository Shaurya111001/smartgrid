#!/bin/bash
# test_kafka_spark_mpi.sh
#
# End-to-end verification of the Kafka -> Spark (analytics-service) -> MPI (simulation)
# pipeline: builds/runs the C++/MPI simulator, confirms its data actually lands in Kafka,
# confirms Spark actually consumes and processes it, and sanity-checks the output.
# Logs a PASS/FAIL line after every step, plus a final summary. Non-zero exit on any failure.
#
# Usage: ./test_kafka_spark_mpi.sh [scenario] [strategy] [districts] [processes]
#   scenario:  sparse (default) | dense
#   strategy:  roundrobin (default) | weighted | nodepartition
#   districts: number of districts to generate (default 2)
#   processes: number of MPI ranks (default 2)
#
# Run from the repository root. Requires: Docker (SmartGrid stack up), mpirun, python3.

set -uo pipefail

SCENARIO="${1:-sparse}"
STRATEGY="${2:-roundrobin}"
DISTRICTS="${3:-2}"
PROCESSES="${4:-2}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIM_APP="$REPO_ROOT/simulation/build/simulation_app"

PASS=0
FAIL=0

ts()   { date "+%Y-%m-%d %H:%M:%S"; }
step() { echo; echo "=== [$(ts)] STEP: $1 ==="; }
ok()   { echo "[$(ts)] PASS: $1"; PASS=$((PASS + 1)); }
bad()  { echo "[$(ts)] FAIL: $1"; FAIL=$((FAIL + 1)); }
info() { echo "[$(ts)] INFO: $1"; }

echo "Kafka -> Spark -> MPI pipeline test"
echo "Config: scenario=$SCENARIO strategy=$STRATEGY districts=$DISTRICTS processes=$PROCESSES"

# ---------------------------------------------------------------------------
step "0. Pre-flight: Docker daemon and containers"
# ---------------------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
  bad "Docker daemon is not running"
  echo "Start Docker Desktop, run 'docker compose up -d' in SmartGrid/, then re-run this script."
  exit 1
fi
ok "Docker daemon is running"

KAFKA_STATUS=$(docker inspect -f '{{.State.Health.Status}}' smartgrid-kafka 2>/dev/null || echo "missing")
if [ "$KAFKA_STATUS" = "healthy" ]; then
  ok "smartgrid-kafka container is healthy"
else
  bad "smartgrid-kafka is not healthy (status: $KAFKA_STATUS)"
  echo "Run 'docker compose up -d' in SmartGrid/ and re-run this script."
  exit 1
fi

ANALYTICS_STATUS=$(docker inspect -f '{{.State.Status}}' analytics-service 2>/dev/null || echo "missing")
if [ "$ANALYTICS_STATUS" = "running" ]; then
  ok "analytics-service container is running"
else
  bad "analytics-service is not running (status: $ANALYTICS_STATUS)"
  echo "Run 'docker compose up -d analytics-service' in SmartGrid/ and re-run this script."
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. Build the MPI simulator (if needed)"
# ---------------------------------------------------------------------------
if [ ! -x "$SIM_APP" ]; then
  info "simulation_app not found, configuring and building..."
  cmake -S "$REPO_ROOT/simulation" -B "$REPO_ROOT/simulation/build" > /tmp/sim_cmake_configure.log 2>&1
  cmake --build "$REPO_ROOT/simulation/build" > /tmp/sim_cmake_build.log 2>&1
fi

if [ -x "$SIM_APP" ]; then
  ok "simulation_app binary exists and is executable"
else
  bad "simulation_app failed to build -- see /tmp/sim_cmake_build.log"
  exit 1
fi

if command -v mpirun >/dev/null 2>&1; then
  ok "mpirun is available on PATH"
else
  bad "mpirun not found -- install with 'brew install open-mpi' and rebuild the simulator"
  exit 1
fi

if grep -q "librdkafka" "$REPO_ROOT/simulation/build/CMakeCache.txt" 2>/dev/null; then
  ok "simulator was configured with librdkafka (Kafka publishing enabled)"
else
  info "could not confirm librdkafka in CMakeCache.txt -- if the run below produces 0 new Kafka messages, rebuild with 'rm -rf simulation/build' first"
fi

# ---------------------------------------------------------------------------
step "2. Snapshot measurement-events before the run"
# ---------------------------------------------------------------------------
BEFORE_COUNT=$(docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic measurement-events --from-beginning --timeout-ms 10000 2>/dev/null | grep -c '"eventType":"MEASUREMENT"')
info "measurement-events currently has $BEFORE_COUNT simulator-originated (MEASUREMENT) message(s)"

RUN_START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")   # trailing Z: without it, 'docker logs --since'
                                                 # interprets a bare timestamp as LOCAL time, not
                                                 # UTC, silently skewing the filter by the host's
                                                 # UTC offset and letting old batches slip through
RESULTS_DIR="$REPO_ROOT/results/${SCENARIO}_${STRATEGY}_p${PROCESSES}"
if [ -d "$RESULTS_DIR" ]; then
  info "clearing previous local results at $RESULTS_DIR for a clean correctness check"
  rm -rf "$RESULTS_DIR"
fi

# ---------------------------------------------------------------------------
step "3. Run the MPI simulator ($PROCESSES ranks, $STRATEGY, $SCENARIO, $DISTRICTS districts)"
# ---------------------------------------------------------------------------
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SIM_LOG="/tmp/sim_run_$(date +%s).log"
if mpirun -np "$PROCESSES" --oversubscribe "$SIM_APP" "$SCENARIO" "$STRATEGY" "$DISTRICTS" > "$SIM_LOG" 2>&1; then
  ok "simulation_app exited cleanly (exit code 0)"
else
  bad "simulation_app exited with a non-zero status -- see $SIM_LOG"
fi

FINISHED_COUNT=$(grep -c "Simulation finished" "$SIM_LOG")
if [ "$FINISHED_COUNT" -eq "$PROCESSES" ]; then
  ok "all $PROCESSES rank(s) logged 'Simulation finished' (no deadlock/hang)"
else
  bad "expected $PROCESSES 'Simulation finished' line(s), got $FINISHED_COUNT -- see $SIM_LOG"
fi
info "full simulator output saved to $SIM_LOG"

# ---------------------------------------------------------------------------
step "4. Verify measurement-events received new data"
# ---------------------------------------------------------------------------
AFTER_COUNT=$(docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic measurement-events --from-beginning --timeout-ms 20000 2>/dev/null | grep -c '"eventType":"MEASUREMENT"')
NEW_MESSAGES=$((AFTER_COUNT - BEFORE_COUNT))
info "measurement-events now has $AFTER_COUNT simulator message(s) ($NEW_MESSAGES new)"
if [ "$NEW_MESSAGES" -gt 0 ]; then
  ok "$NEW_MESSAGES new message(s) landed in measurement-events"
else
  bad "no new messages detected in measurement-events -- Kafka publish likely failed (check simulator was built with librdkafka)"
fi

# ---------------------------------------------------------------------------
step "5. Verify analytics-service (Spark) actually processed the new batch"
# ---------------------------------------------------------------------------
sleep 5   # give Spark a moment to trigger a micro-batch
PROGRESS_LINES=$(docker logs analytics-service --since "$RUN_START_TS" 2>&1 | grep "Query progress" | grep -v "inputRows=0")
if [ -n "$PROGRESS_LINES" ]; then
  ok "analytics-service logged at least one micro-batch with inputRows > 0 since the run started"
  echo "$PROGRESS_LINES" | tail -5
else
  bad "no 'Query progress' line with inputRows > 0 found since the run started"
fi

# ---------------------------------------------------------------------------
step "6. Verify district-soc-state output is present and bounded"
# ---------------------------------------------------------------------------
SOC_OUTPUT=$(docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic district-soc-state --from-beginning --timeout-ms 15000 2>/dev/null | tail -10)
if [ -n "$SOC_OUTPUT" ]; then
  ok "district-soc-state has output"
  echo "$SOC_OUTPUT"
  MAX_ABS=$(echo "$SOC_OUTPUT" | python3 -c "
import json, sys
maxv = 0.0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        v = abs(json.loads(line).get('current_SOC', 0.0))
        maxv = max(maxv, v)
    except Exception:
        pass
print(maxv)
")
  info "largest |current_SOC| observed in the last 10 messages: $MAX_ABS"
  if python3 -c "exit(0 if $MAX_ABS < 100000 else 1)"; then
    ok "current_SOC magnitude is within a sane bound (< 100000)"
  else
    bad "current_SOC magnitude ($MAX_ABS) looks unbounded -- possible regression to the old sum-based SOC bug"
  fi
else
  bad "district-soc-state has no output at all"
fi

# ---------------------------------------------------------------------------
if [ "$STRATEGY" = "nodepartition" ]; then
  step "7. Cross-check MPI_Allreduce correctness (node-partition only)"
  # -------------------------------------------------------------------------
  if [ -f "$RESULTS_DIR/node_deltas.csv" ] && [ -f "$RESULTS_DIR/district_aggregate.csv" ]; then
    MISMATCH=0
    while IFS=',' read -r step_no district_id aggregate; do
      manual_sum=$(awk -F',' -v s="$step_no" -v d="$district_id" \
        '$1==s && $2==d {sum+=$4} END{print sum+0}' "$RESULTS_DIR/node_deltas.csv")
      if ! python3 -c "exit(0 if abs($aggregate - $manual_sum) < 0.01 else 1)" 2>/dev/null; then
        bad "step=$step_no district=$district_id: recorded=$aggregate manual_sum=$manual_sum (MISMATCH)"
        MISMATCH=1
      fi
    done < "$RESULTS_DIR/district_aggregate.csv"
    if [ "$MISMATCH" -eq 0 ]; then
      ok "MPI_Allreduce-combined aggregate matches an independent manual per-node sum for every (step, district)"
    fi
  else
    bad "expected result files not found under $RESULTS_DIR"
  fi
else
  info "Skipping step 7 (Allreduce cross-check only applies to the nodepartition strategy)"
fi

# ---------------------------------------------------------------------------
echo
echo "================= SUMMARY ================="
echo "PASS: $PASS   FAIL: $FAIL"
echo
echo "Note: district-window-stats was not checked here -- it only emits after Spark's"
echo "5-minute watermark passes, well beyond this script's runtime. To check it later:"
echo "  docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \\"
echo "    --topic district-window-stats --from-beginning --timeout-ms 15000"
echo "============================================="

if [ "$FAIL" -eq 0 ]; then
  echo "Kafka -> Spark -> MPI pipeline verified successfully."
  exit 0
else
  echo "One or more checks failed -- see FAIL lines above."
  exit 1
fi

#!/usr/bin/env bash
# Harness run by the planner: dedicated network/containers, 4 bounded jobs and unbounded dedup recovery.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
py="$repo/.venv/bin/python"
run_dir=${1:-"$repo/out/order-lifecycle-stream-$(date +%Y%m%dT%H%M%S)-$$"}
mkdir "$run_dir"
run_dir=$(cd "$run_dir" && pwd)
prefix="platform-v2-$$"
network="$prefix-net"
kafka="$prefix-kafka"
jm="$prefix-jm"
tm="$prefix-tm"
client_pid=''
control_pid=''
cleanup() {
  # Clean up exactly the resources the harness created; keep evidence files even on failure.
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$client_pid" ]]; then kill "$client_pid" 2>/dev/null || true; fi
  if [[ -n "$control_pid" ]]; then kill "$control_pid" 2>/dev/null || true; fi
  for container in "$jm" "$tm" "$kafka"; do
    docker logs "$container" >"$run_dir/$container.log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  docker network rm "$network" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# No implicit pull/build: the planner prepares both images before running.
docker image inspect apache/kafka:3.9.1 platform-flink:1.20.3-kafka3.4.0 >/dev/null
"$py" -m generator.generate_order_lifecycle --output "$run_dir/source" --orders-per-day 200 --days 10 --seed 7 >"$run_dir/source.log"
mkdir "$run_dir/output" "$run_dir/checkpoints"
chmod 777 "$run_dir/output" "$run_dir/checkpoints"
docker network create "$network" >/dev/null
# KRaft does not allow changing advertised.listeners after start ⇒ pick a free host port FIRST and advertise exactly that port.
port=$("$py" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
docker run -d --pull never --name "$kafka" --network "$network" --network-alias kafka -p "127.0.0.1:$port:9094" \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
 -e KAFKA_LISTENERS=INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094 \
 -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://127.0.0.1:$port" \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
 -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:3.9.1 >/dev/null
for attempt in $(seq 1 60); do
 if docker exec "$kafka" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >"$run_dir/kafka-ready.log" 2>&1; then break; fi
 sleep 1
done
for feed in carrier.delivery_events seller_postsales.return_events seller_postsales.refund_events seller_wms.stock_operations; do
 docker exec "$kafka" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic "order_lifecycle.$feed" --partitions 3 --replication-factor 1 >/dev/null
 docker exec "$kafka" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic "order_lifecycle.late.$feed" --partitions 1 --replication-factor 1 >/dev/null
done
for epoch in 1 2; do
 "$py" -m generator.publish_order_lifecycle_events --source "$run_dir/source" --bootstrap "127.0.0.1:$port" --epoch "$epoch" --manifest "$run_dir/stream-$epoch.json" --late-topics
 "$py" -m generator.build_order_lifecycle_stream_oracle --source "$run_dir/source" --manifest "$run_dir/stream-$epoch.json" --output "$run_dir/oracle-$epoch.json"
 "$py" -m generator.build_order_lifecycle_stream_oracle --source "$run_dir/source" --manifest "$run_dir/stream-$epoch-late.json" --output "$run_dir/oracle-$epoch-late.json"
 restart_flags=()
 if [[ "$epoch" == 2 ]]; then restart_flags=(--restart-mode); fi
 "$py" scripts/run/render_order_lifecycle_stream.py "${restart_flags[@]}" --output "$run_dir/sql-$epoch" --epoch "$epoch" --manifest "$run_dir/stream-$epoch.json" --checkpoint-ms 100
 "$py" scripts/run/render_order_lifecycle_stream.py --output "$run_dir/sql-$epoch-late" --epoch "$epoch" --manifest "$run_dir/stream-$epoch-late.json" --late-topics --checkpoint-ms 100
 done
props=$'jobmanager.rpc.address: flink-jobmanager\nrest.address: flink-jobmanager\njobmanager.memory.process.size: 1024m\ntaskmanager.memory.process.size: 1536m\ntaskmanager.numberOfTaskSlots: 2\nrestart-strategy.type: fixed-delay\nrestart-strategy.fixed-delay.attempts: 5\nrestart-strategy.fixed-delay.delay: 1s\nheartbeat.timeout: 10000'
mounts=(--mount "type=bind,src=$run_dir/output,dst=/opt/flink/output" --mount "type=bind,src=$run_dir/checkpoints,dst=/opt/flink/checkpoints" --mount "type=bind,src=$run_dir,dst=/harness,readonly")
docker run -d --pull never --name "$jm" --network "$network" --network-alias flink-jobmanager -p 127.0.0.1::8081 \
 -e "FLINK_PROPERTIES=$props" "${mounts[@]}" platform-flink:1.20.3-kafka3.4.0 jobmanager >/dev/null
docker run -d --pull never --name "$tm" --network "$network" -e "FLINK_PROPERTIES=$props" "${mounts[@]}" \
 platform-flink:1.20.3-kafka3.4.0 taskmanager >/dev/null
rest_port=$(docker port "$jm" 8081/tcp | sed 's/.*://')
for attempt in $(seq 1 60); do
 if "$py" -c 'import json,sys,urllib.request; assert json.load(urllib.request.urlopen(sys.argv[1],timeout=2))["taskmanagers"]' "http://127.0.0.1:$rest_port/overview" 2>/dev/null; then break; fi
 sleep 1
done
for epoch in 1 2; do
 for job in baseline late_arrival dedup windows; do
  if [[ "$epoch/$job" == '2/dedup' ]]; then
   "$py" tests/harness/control_order_lifecycle_stream.py --base "http://127.0.0.1:$rest_port" --taskmanager "$tm" --evidence "$run_dir/restart.json" --oracle "$run_dir/oracle-$epoch.json" --output "$run_dir/output/order_lifecycle" >"$run_dir/restart.log" 2>&1 &
   control_pid=$!
  fi
  timeout 600 docker exec -u flink "$jm" /opt/flink/bin/sql-client.sh -f "/harness/sql-$epoch/$job.sql" >"$run_dir/$epoch-$job.log" 2>&1 &
  client_pid=$!
  wait "$client_pid"
  client_pid=''
  if [[ -n "$control_pid" ]]; then
   wait "$control_pid"
   control_pid=''
   # Streaming is cancelled; the separate bounded summary keeps the existing count reconciliation.
   timeout 600 docker exec -u flink "$jm" /opt/flink/bin/sql-client.sh -f "/harness/sql-$epoch/dedup_counts.sql" >"$run_dir/$epoch-dedup_counts.log" 2>&1
  fi
 done
 for sql in "$run_dir/sql-$epoch-late/"*.sql; do
  job=$(basename "$sql")
  timeout 600 docker exec -u flink "$jm" /opt/flink/bin/sql-client.sh -f "/harness/sql-$epoch-late/$job" >"$run_dir/$epoch-proof-$job.log" 2>&1
 done
 "$py" scripts/evidence/compare_order_lifecycle_stream.py --late-oracle "$run_dir/oracle-$epoch-late.json" --flink-output "$run_dir/output/order_lifecycle" --oracle "$run_dir/oracle-$epoch.json" --output "$run_dir/compare-$epoch.json"
done
printf 'Evidence: %s\n' "$run_dir"

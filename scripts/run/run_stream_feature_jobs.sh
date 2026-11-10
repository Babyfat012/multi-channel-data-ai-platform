#!/usr/bin/env bash
# Submit TWO PyFlink stream feature jobs to the Flink ML cluster (2.2): ml-sku-return-24h-online and -offline.
# Same file flink_jobs/ml/sku_return_feature.py, different --sink flag. A running job with the same name is cancelled first ⇒ redeploys
# never create duplicate jobs. COMPOSE defaults to the machine's COMPOSE_FILE (EC2 presets the AWS overlay).
set -euo pipefail
cd "$(dirname "$0")/../.."
read -ra C <<<"${COMPOSE:-docker compose --profile stream --profile flink-ml}"
topic=order_snapshots.return_events
# Records carry (past) event time: without retention.ms=-1, Kafka deletes them immediately by timestamp.
"${C[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$topic" --partitions 3 --replication-factor 1 --config retention.ms=-1
"${C[@]}" exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type topics --entity-name "$topic" --add-config retention.ms=-1
rest="http://localhost:${FLINK_ML_UI_PORT:-8086}"
for sink in online offline; do
  name="ml-sku-return-24h-$sink"
  for id in $(curl -fsS "$rest/jobs/overview" | python3 -c "import json,sys
for j in json.load(sys.stdin)['jobs']:
    if j['name'] == '$name' and j['state'] == 'RUNNING': print(j['jid'])"); do
    curl -fsS -X PATCH "$rest/jobs/$id?mode=cancel" >/dev/null && echo "cancelled old job $name $id"
  done
  "${C[@]}" exec -T flink-ml-jobmanager ./bin/flink run -d -py /opt/flink/jobs/ml/sku_return_feature.py --sink "$sink"
done

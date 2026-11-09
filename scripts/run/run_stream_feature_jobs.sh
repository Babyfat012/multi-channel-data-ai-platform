#!/usr/bin/env bash
# Submit HAI job PyFlink stream feature lên cluster Flink ML (2.2): ml-sku-return-24h-online và -offline.
# Cùng file flink_jobs/ml/sku_return_feature.py, khác cờ --sink. Job cùng tên đang chạy bị huỷ trước ⇒ deploy lại
# không sinh job trùng. COMPOSE mặc định dùng COMPOSE_FILE của máy (EC2 đặt sẵn overlay AWS).
set -euo pipefail
cd "$(dirname "$0")/../.."
read -ra C <<<"${COMPOSE:-docker compose --profile stream --profile flink-ml}"
topic=order_snapshots.return_events
# Bản ghi mang event time (quá khứ): không có retention.ms=-1 thì Kafka xoá ngay theo timestamp.
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
    curl -fsS -X PATCH "$rest/jobs/$id?mode=cancel" >/dev/null && echo "đã huỷ job cũ $name $id"
  done
  "${C[@]}" exec -T flink-ml-jobmanager ./bin/flink run -d -py /opt/flink/jobs/ml/sku_return_feature.py --sink "$sink"
done

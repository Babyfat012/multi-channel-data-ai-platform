"""Kafka incarnation (cluster id + topic UUID) and exclusive offset endpoints of the Walmart CDC topic.

The nightly gate (spark_jobs/cdc/validate_walmart_order_changes.py) captures them once, then checks that
Bronze holds every event in that range.
"""

from __future__ import annotations

import json


def _validate(snapshot):
    """Reject a boundary without topic ids or with an invalid offset range per partition."""
    for key in ("topic", "cluster_id", "topic_id"):
        if not isinstance(snapshot.get(key), str) or not snapshot[key].strip():
            raise ValueError(f"Missing Kafka {key}")
    starts, ends = snapshot.get("starting_offsets"), snapshot.get("ending_offsets")
    if (
        not isinstance(starts, dict)
        or not isinstance(ends, dict)
        or not starts
        or starts.keys() != ends.keys()
    ):
        raise ValueError("Offsets must cover the same nonempty partition set")
    for partition, start in starts.items():
        end = ends[partition]
        if (
            not isinstance(partition, str)
            or not partition.isdecimal()
            or str(int(partition)) != partition
            or type(start) is not int
            or type(end) is not int
            or not 0 <= start <= end
        ):
            raise ValueError("Invalid Kafka offset range")


def kafka_read_options(boundary):
    """Use explicit partitions, including an empty partition's numeric endpoint."""
    _validate(boundary)
    topic = boundary["topic"]
    return {
        "assign": json.dumps({topic: sorted(map(int, boundary["starting_offsets"]))}),
        "startingOffsets": json.dumps({topic: boundary["starting_offsets"]}),
        "endingOffsets": json.dumps({topic: boundary["ending_offsets"]}),
        "kafka.isolation.level": "read_committed",
        "failOnDataLoss": "true",
    }


def capture_kafka_boundary(spark, bootstrap, topic):
    """Capture cluster/topic identity and committed endpoints using bundled clients.

    No consumer group offsets are changed. Topic UUID requires Kafka 2.8+.
    Source recreation during capture fails rather than mixing incarnations.
    """
    jvm = spark.sparkContext._jvm
    properties = jvm.java.util.Properties()
    for key, value in {
        "bootstrap.servers": bootstrap,
        "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "isolation.level": "read_committed",
        "enable.auto.commit": "false",
        "allow.auto.create.topics": "false",
        "default.api.timeout.ms": "30000",
        "request.timeout.ms": "30000",
    }.items():
        properties.setProperty(key, value)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
    consumer = None
    try:
        topics = jvm.java.util.ArrayList()
        topics.add(topic)
        description = admin.describeTopics(topics).allTopicNames().get().get(topic)
        topic_id = str(description.topicId())
        if topic_id == str(jvm.org.apache.kafka.common.Uuid.ZERO_UUID):
            raise ValueError("Kafka broker did not expose a topic UUID")
        cluster_id = str(admin.describeCluster().clusterId().get())
        partitions = jvm.java.util.ArrayList()
        for info in description.partitions():
            partitions.add(jvm.org.apache.kafka.common.TopicPartition(topic, info.partition()))
        consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(properties)
        starts, ends = consumer.beginningOffsets(partitions), consumer.endOffsets(partitions)
        observed = {
            "topic": topic,
            "cluster_id": cluster_id,
            "topic_id": topic_id,
            "starting_offsets": {str(p.partition()): int(starts.get(p)) for p in partitions},
            "ending_offsets": {str(p.partition()): int(ends.get(p)) for p in partitions},
        }
        if str(admin.describeTopics(topics).allTopicNames().get().get(topic).topicId()) != topic_id:
            raise ValueError("Kafka topic recreated during boundary capture")
        _validate(observed)
        return observed
    finally:
        if consumer is not None:
            consumer.close()
        admin.close()


def gate_failures(
    source: int, rows: int, unique: int, missing: int, extra: int, mismatched: int, invalid: int
) -> list[str]:
    """Why Bronze does not match the captured Kafka range; empty means PASS. An empty range passes (no changes)."""
    checks = {
        "events missing from Bronze": missing,
        "Bronze rows not in Kafka": extra,
        "duplicate offsets": rows - unique,
        "payload differs from Kafka": mismatched,
        "malformed rows": invalid,
        "row count differs from Kafka": rows != source,
    }
    return [name for name, bad in checks.items() if bad]

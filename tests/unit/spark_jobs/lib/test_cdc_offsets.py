"""The CDC gate reads exactly the captured Kafka range, and refuses a malformed one."""

import copy
import json

import pytest

from spark_jobs.lib.cdc_offsets import kafka_read_options


def source_snapshot(**changes):
    """A captured Kafka incarnation/range."""
    return {
        "topic": "orders",
        "cluster_id": "cluster-A",
        "topic_id": "topic-A",
        "starting_offsets": {"0": 0, "1": 2},
        "ending_offsets": {"0": 5, "1": 7},
        **changes,
    }


def test_read_options_pin_every_partition_to_the_captured_range():
    """Explicit partitions and both endpoints: Kafka growth during the check never widens the range."""
    options = kafka_read_options(source_snapshot())
    assert json.loads(options["assign"]) == {"orders": [0, 1]}
    assert json.loads(options["startingOffsets"]) == {"orders": {"0": 0, "1": 2}}
    assert json.loads(options["endingOffsets"]) == {"orders": {"0": 5, "1": 7}}
    assert options["failOnDataLoss"] == "true"


@pytest.mark.parametrize("start,end", [(-1, 5), (3, 2), (True, 5)])
def test_invalid_offsets_are_rejected(start, end):
    """No sentinel/latest offsets or reversed ranges."""
    snapshot = copy.deepcopy(source_snapshot())
    snapshot["starting_offsets"]["0"] = start
    snapshot["ending_offsets"]["0"] = end
    with pytest.raises(ValueError):
        kafka_read_options(snapshot)


@pytest.mark.parametrize("missing", ["topic_id", "cluster_id"])
def test_a_range_without_its_source_identity_is_rejected(missing):
    """Without cluster/topic ids a recreated topic's offsets could be mistaken for the old ones."""
    with pytest.raises(ValueError, match=missing):
        kafka_read_options(source_snapshot(**{missing: ""}))


def test_gate_passes_only_an_exact_copy():
    """Same count, unique offsets, nothing missing/extra/changed/malformed ⇒ PASS; an idle topic passes too."""
    from spark_jobs.lib.cdc_offsets import gate_failures

    assert gate_failures(5, 5, 5, 0, 0, 0, 0) == []
    assert gate_failures(0, 0, 0, 0, 0, 0, 0) == []
    assert gate_failures(5, 4, 4, 1, 0, 0, 0) == [
        "events missing from Bronze",
        "row count differs from Kafka",
    ]
    assert gate_failures(5, 6, 5, 0, 0, 0, 0) == [
        "duplicate offsets",
        "row count differs from Kafka",
    ]
    assert gate_failures(5, 5, 5, 0, 0, 1, 2) == ["payload differs from Kafka", "malformed rows"]

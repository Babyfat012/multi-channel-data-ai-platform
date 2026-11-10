"""Tests for the write layer: the incremental writer guarantees the Batch part relies on.

These guarantees are why `BatchRun` can drop its record buffers and trust the stamps. If one breaks,
100 GB-scale output is corrupted after hours of running, so they are pinned here rather
than only checked through a small Batch run.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from generator.storage import OutputStore

SCHEMA = pa.schema([("n", pa.int64())])


def test_jsonl_pool_reports_bytes_matching_the_file_on_disk(tmp_path):
    """`close` returns the actual byte count, since the manifest takes `datasets[*].bytes` from it."""
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        pool.write("a", "a.jsonl", {"x": 1})
        pool.write("a", "a.jsonl", {"x": "é"})
        rows, written = pool.close("a")

    on_disk = (tmp_path / "a.jsonl").stat().st_size
    assert (rows, written) == (2, on_disk)


def test_jsonl_pool_keeps_files_interleaved_without_mixing_them(tmp_path):
    """Many files open at once receive interleaved rows without mixing them up."""
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        for i in range(3):
            pool.write("a", "a.jsonl", {"n": i})
            pool.write("b", "b.jsonl", {"n": -i})

    def read(name: str) -> list[int]:
        """Values of column `n` read back from a JSONL file, to compare contents."""
        return [json.loads(line)["n"] for line in (tmp_path / name).read_text().splitlines()]

    assert read("a.jsonl") == [0, 1, 2]
    assert read("b.jsonl") == [0, -1, -2]


def test_jsonl_pool_closes_open_files_when_the_body_raises(tmp_path):
    """An error midway still closes the files, so written rows can be read back.

    This is why the boundary "Storage owns closing files" was chosen: a 100 GB run
    takes several hours, and generated data should not be lost to an error at the end.
    """
    store = OutputStore(str(tmp_path))
    with pytest.raises(RuntimeError), store.jsonl_writers() as pool:
        pool.write("a", "a.jsonl", {"x": 1})
        raise RuntimeError("simulated day failed")

    assert [json.loads(line) for line in (tmp_path / "a.jsonl").read_text().splitlines()] == [
        {"x": 1}
    ]


def test_jsonl_pool_refuses_to_write_to_a_closed_key(tmp_path):
    """Writing again to a closed key must raise, because reopening would wipe the file.

    `BatchRun._flush` only closes export days <= the day just generated, so this does not happen today.
    This test is the safety net: if a backdated export kind appears later, the test fails right away instead of
    losing a day of data after hours of running.
    """
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        pool.write("a", "a.jsonl", {"x": 1})
        pool.close("a")
        with pytest.raises(ValueError, match="already closed"):
            pool.write("a", "a.jsonl", {"x": 2})

    assert (tmp_path / "a.jsonl").read_text().count("\n") == 1


def test_parquet_writer_makes_one_row_group_per_write_and_skips_empty(tmp_path):
    """Each `write` is one row group; empty blocks are skipped so the metadata does not bloat."""
    store = OutputStore(str(tmp_path))
    with store.parquet_writer("t.parquet", SCHEMA) as writer:
        assert writer.write({"n": [1, 2]}) == 2
        assert writer.write({"n": []}) == 0
        assert writer.write({"n": [3]}) == 1
        assert (writer.rows, writer.row_groups) == (3, 2)

    opened = pq.ParquetFile(tmp_path / "t.parquet")
    assert opened.num_row_groups == 2
    assert opened.read().column("n").to_pylist() == [1, 2, 3]


def test_parquet_writer_closes_the_file_when_the_body_raises(tmp_path):
    """The Parquet footer is written even if the block body fails, so the file stays readable."""
    store = OutputStore(str(tmp_path))
    with pytest.raises(RuntimeError), store.parquet_writer("t.parquet", SCHEMA) as writer:
        writer.write({"n": [1]})
        raise RuntimeError("simulated day failed")

    assert pq.read_table(tmp_path / "t.parquet").column("n").to_pylist() == [1]


def test_write_json_atomic_leaves_no_temporary_file_behind(tmp_path):
    """After writing a stamp no `.tmp` remains, because the stamp is renamed into place, not copied."""
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("_checkpoint/day-00003.json", {"rows": 7})

    assert store.read_json("_checkpoint/day-00003.json") == {"rows": 7}
    assert not list(tmp_path.rglob("*.tmp"))


def test_write_json_atomic_replaces_an_existing_file_completely(tmp_path):
    """A new stamp fully replaces the old one, leaving no tail of longer previous content.

    `BatchRun` rewrites the same day's stamp on rerun, and the old stamp may be longer than
    the new one. If overwriting only wrote over the start of the file, the old tail would remain and break the JSON.
    """
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("_checkpoint/day-00001.json", {"long": "x" * 500})
    store.write_json_atomic("_checkpoint/day-00001.json", {"n": 1})

    assert store.read_json("_checkpoint/day-00001.json") == {"n": 1}


def test_write_json_atomic_keeps_floats_exact_through_a_round_trip(tmp_path):
    """Floats must round-trip through the stamp unchanged, since the discount total accumulates through stamps.

    `amazon_discount_sum` is a `float` accumulated over the whole run. One bit off here and
    the manifest of an interrupted run differs from an uninterrupted one, and the byte comparison fails —
    so this test spells out why it does not fail.
    """
    store = OutputStore(str(tmp_path))
    value = 1234.5678901234567
    store.write_json_atomic("x.json", {"discount_sum": value})

    assert store.read_json("x.json")["discount_sum"] == value


def test_remove_is_quiet_when_the_file_is_already_gone(tmp_path):
    """Cleaning up the cut day's files must tolerate the files already being gone."""
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("a.json", {})
    store.remove("a.json")
    store.remove("a.json")

    assert store.glob("a.json") == []

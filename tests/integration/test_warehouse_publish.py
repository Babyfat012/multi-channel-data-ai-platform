"""dp4 warehouse write-audit-publish (gap D8) against a disposable local PostgreSQL; opt in with
PLATFORM_TEST_POSTGRES_DSN, e.g. postgresql://postgres:test@127.0.0.1:55432/postgres."""

import os
import threading
import time
from urllib.parse import urlsplit

import pytest

from spark_jobs.lib.label_channels import CHANNELS
from spark_jobs.lib.order_lifecycle_warehouse import connect
from spark_jobs.warehouse.export_order_snapshot_lines import drop_stale_stages, publish

TABLE = "fact_lines_test"
GOLD = {channel: 2 for channel in CHANNELS}


@pytest.fixture
def dsn():
    """Only a local, throwaway database: the tests drop and create schema `warehouse`."""
    url = os.environ.get("PLATFORM_TEST_POSTGRES_DSN")
    if not url:
        pytest.skip("set PLATFORM_TEST_POSTGRES_DSN to a disposable PostgreSQL")
    assert urlsplit(url).hostname in ("127.0.0.1", "localhost"), "test PostgreSQL must be local"
    conn = connect(url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
        cur.execute("CREATE SCHEMA warehouse")
    conn.close()
    return url


def fill(dsn, table, per_channel, tag):
    """A table shaped like the export, `per_channel` rows per Channel; `tag` marks which load wrote it."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE warehouse.{table} (channel text, seller_sku text, tag text)")
        for channel, n in per_channel.items():
            for i in range(n):
                cur.execute(
                    f"INSERT INTO warehouse.{table} VALUES (%s, %s, %s)", (channel, f"sku-{i}", tag)
                )


def serving(dsn):
    """(row count, tags) of the serving table as a fresh reader sees it."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*), array_agg(DISTINCT tag) FROM warehouse.{TABLE}")
        return cur.fetchone()


def tables(dsn):
    """Table names in schema warehouse."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'warehouse' ORDER BY 1")
        return [r[0] for r in cur.fetchall()]


def started(target, *args):
    """Daemon thread (not started yet): a stuck swap after a failed assert must not hang pytest at exit."""
    return threading.Thread(target=target, args=args, daemon=True)


def test_publish_swaps_in_the_reconciled_stage_with_its_index(dsn):
    """Old serving data is replaced by the staged load; the index came along; the stage name is gone."""
    fill(dsn, TABLE, {c: 1 for c in CHANNELS}, "old")
    fill(dsn, f"{TABLE}__load_a", GOLD, "new")
    conn = connect(dsn)
    assert publish(conn, TABLE, f"{TABLE}__load_a", GOLD) == GOLD
    conn.close()
    assert serving(dsn) == (10, ["new"])
    assert tables(dsn) == [TABLE]
    with connect(dsn) as c, c.cursor() as cur:
        cur.execute("SELECT indexdef FROM pg_indexes WHERE tablename = %s", (TABLE,))
        assert "(seller_sku)" in cur.fetchone()[0]


def test_failed_reconciliation_leaves_serving_untouched(dsn):
    """A short Channel in staging ⇒ error before the swap; readers keep the old, complete table."""
    fill(dsn, TABLE, GOLD, "old")
    fill(dsn, f"{TABLE}__load_b", GOLD | {"shopee": 1}, "new")
    conn = connect(dsn)
    with pytest.raises(ValueError, match="shopee"):
        publish(conn, TABLE, f"{TABLE}__load_b", GOLD)
    conn.close()
    assert serving(dsn) == (10, ["old"])


def test_first_load_creates_the_serving_table(dsn):
    """Brand-new database (AWS): nothing to drop, the stage simply becomes the table."""
    fill(dsn, f"{TABLE}__load_c", GOLD, "new")
    conn = connect(dsn)
    publish(conn, TABLE, f"{TABLE}__load_c", GOLD)
    conn.close()
    assert serving(dsn) == (10, ["new"])


def test_reader_never_sees_an_empty_table(dsn):
    """A reader mid-transaction keeps the old complete data; the swap waits for it, then the next read is
    the new complete data. Under the old overwrite, this reader would have seen an empty table."""
    fill(dsn, TABLE, GOLD, "old")
    fill(dsn, f"{TABLE}__load_d", GOLD, "new")
    reader, writer = connect(dsn), connect(dsn)
    swap = started(publish, writer, TABLE, f"{TABLE}__load_d", GOLD)
    try:
        with reader.cursor() as cur:  # psycopg opens a transaction: holds ACCESS SHARE on the table
            cur.execute(f"SELECT count(*) FROM warehouse.{TABLE}")
            assert cur.fetchone()[0] == 10
        swap.start()
        time.sleep(0.5)
        assert swap.is_alive(), "the swap must wait for the open reader"
        with reader.cursor() as cur:
            cur.execute(f"SELECT count(*), array_agg(DISTINCT tag) FROM warehouse.{TABLE}")
            assert cur.fetchone() == (10, ["old"])
    finally:
        reader.close()  # releases the lock even when an assert failed, so the swap thread can end
        swap.join(timeout=10)
        writer.close()
    assert serving(dsn) == (10, ["new"])


def test_drop_stale_stages_only_drops_this_tables_stages(dsn):
    """`_` is a LIKE wildcard: escaped, so a look-alike name (xx instead of __) survives."""
    for name in (f"{TABLE}__load_old1", f"{TABLE}__load_old2", f"{TABLE}xxloadxkeep", TABLE):
        fill(dsn, name, {"amazon": 1}, "x")
    conn = connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        drop_stale_stages(cur, TABLE)
    conn.close()
    assert tables(dsn) == [TABLE, f"{TABLE}xxloadxkeep"]


def test_new_reader_waits_through_the_swap_instead_of_failing(dsn):
    """DROP + RENAME must commit together. Hold the RENAME back (lock on the stage) right after the DROP:
    a new reader must wait and then get the new table, not 'relation does not exist' in between."""
    fill(dsn, TABLE, GOLD, "old")
    fill(dsn, f"{TABLE}__load_e", GOLD, "new")
    blocker, writer = connect(dsn), connect(dsn)
    seen = []

    def read():
        """A reader arriving mid-swap; records the result or the error."""
        try:
            seen.append(serving(dsn))
        except Exception as error:
            seen.append(type(error).__name__)

    swap = started(publish, writer, TABLE, f"{TABLE}__load_e", GOLD)
    reader = started(read)
    try:
        with (
            blocker.cursor() as cur
        ):  # ACCESS SHARE on the stage: RENAME (ACCESS EXCLUSIVE) must wait
            cur.execute(f"SELECT 1 FROM warehouse.{TABLE}__load_e LIMIT 1")
        swap.start()
        time.sleep(0.5)  # publish is now stuck at the RENAME, after its DROP
        reader.start()
        time.sleep(0.5)
        assert seen == [], f"the reader must wait for the swap, got {seen}"
    finally:
        blocker.close()  # releases the stage lock even when an assert failed
        swap.join(timeout=10)
        reader.join(timeout=10)
        writer.close()
    assert seen == [(10, ["new"])]


def test_swap_gives_up_on_a_long_reader_and_keeps_the_old_table(dsn):
    """A reader that never finishes: each attempt waits lock_timeout, then the job fails with the lock error
    instead of queueing every other reader behind it forever; the old table is still served."""
    from spark_jobs.warehouse.export_order_snapshot_lines import lock_not_available

    fill(dsn, TABLE, GOLD, "old")
    fill(dsn, f"{TABLE}__load_f", GOLD, "new")
    reader, writer = connect(dsn), connect(dsn)
    try:
        with reader.cursor() as cur:  # open transaction = a long query
            cur.execute(f"SELECT count(*) FROM warehouse.{TABLE}")
        began = time.monotonic()
        with pytest.raises(Exception) as caught:
            publish(writer, TABLE, f"{TABLE}__load_f", GOLD, lock_timeout="200ms", pause=0)
        assert lock_not_available(caught.value)
        assert 0.6 <= time.monotonic() - began < 5  # 3 attempts × 200 ms, not an endless wait
    finally:
        reader.close()
        writer.close()
    assert serving(dsn) == (10, ["old"])


def test_swap_succeeds_on_a_later_attempt_once_the_reader_ends(dsn):
    """The reader ends during the retries: a later attempt gets the lock and publishes."""
    fill(dsn, TABLE, GOLD, "old")
    fill(dsn, f"{TABLE}__load_g", GOLD, "new")
    reader, writer = connect(dsn), connect(dsn)
    with reader.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM warehouse.{TABLE}")
    ender = threading.Timer(0.3, reader.close)  # after the first attempt (200 ms) has failed
    ender.start()
    try:
        publish(writer, TABLE, f"{TABLE}__load_g", GOLD, lock_timeout="200ms", pause=0.3)
    finally:
        ender.join()
        writer.close()
    assert serving(dsn) == (10, ["new"])

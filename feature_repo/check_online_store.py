"""Guard used by publish_batch: stop a run from reporting green when Redis is empty but Feast remembers an old batch load."""

VIEW = "sku_return_daily"


def check_incremental_online_store(store, redis):
    """Read the watermark of the right view and Redis DB; connection/registry errors must fail the task."""
    last_end = store.get_feature_view(VIEW).most_recent_end_time
    key_count = redis.dbsize()
    print(f"Incremental guard: view={VIEW}, last_end={last_end}, redis_keys={key_count}")
    if key_count == 0 and last_end is not None:
        raise RuntimeError(
            f"Redis is empty but Feast view {VIEW} has materialized up to {last_end}. "
            "Stopping incremental to avoid loading 0 rows while reporting green. "
            "Investigate the cause; deliberately rerun with full_reload=true to restore the batch."
        )

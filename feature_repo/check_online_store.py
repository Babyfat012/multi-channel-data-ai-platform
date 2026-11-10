"""Stop an incremental run from reporting green when Redis is empty but Feast remembers an old batch load."""

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


def main():
    """Use the same Feast 0.66.0 config/client to check the right endpoint and Redis database."""
    from feast import FeatureStore
    from feast.infra.online_stores.redis import RedisOnlineStore

    store = FeatureStore(repo_path=".")
    # Adapter for the pinned Feast: keeps materialize's way of reading host/port/db/auth.
    redis = RedisOnlineStore()._get_client(store.config.online_store)
    try:
        check_incremental_online_store(store, redis)
    finally:
        redis.close()


if __name__ == "__main__":
    main()

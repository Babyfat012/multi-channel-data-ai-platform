"""Chặn incremental báo xanh khi Redis rỗng nhưng Feast nhớ lần nạp batch cũ."""

VIEW = "sku_return_daily"


def check_incremental_online_store(store, redis):
    """Đọc mốc đúng view và DB Redis; lỗi kết nối/registry phải làm task thất bại."""
    last_end = store.get_feature_view(VIEW).most_recent_end_time
    key_count = redis.dbsize()
    print(f"Incremental guard: view={VIEW}, last_end={last_end}, redis_keys={key_count}")
    if key_count == 0 and last_end is not None:
        raise RuntimeError(
            f"Redis rỗng nhưng Feast view {VIEW} đã materialize đến {last_end}. "
            "Dừng incremental để tránh nạp 0 dòng mà báo xanh. "
            "Kiểm tra nguyên nhân; chủ động chạy lại với full_reload=true để phục hồi batch."
        )


def main():
    """Dùng cùng config/client Feast 0.66.0 để kiểm đúng endpoint và Redis database."""
    from feast import FeatureStore
    from feast.infra.online_stores.redis import RedisOnlineStore

    store = FeatureStore(repo_path=".")
    # Adapter theo Feast đã pin: giữ nguyên cách đọc host/port/db/auth của materialize.
    redis = RedisOnlineStore()._get_client(store.config.online_store)
    try:
        check_incremental_online_store(store, redis)
    finally:
        redis.close()


if __name__ == "__main__":
    main()

"""Feature API: SKU return features from the Feast online store (Redis).

Owns the feature-store data: the only service that reads Feast/Redis for SKU features.
"""

import asyncio
import os

from fastapi import APIRouter, HTTPException, Request

from feature_store.publication import daily_status, load_published_store
from serving.schemas import FeatureRequest, FeatureResponse, SkuFeatures

# Look up each view separately: if one view fails or is not registered, its features are None
# instead of failing the whole request.
VIEWS = {
    "sku_return_daily": ["labeled_count_90d", "positive_count_90d", "return_or_refund_rate_90d"],
    "sku_return_24h": ["approved_returns_24h"],
}

router = APIRouter()


def load_store():
    """Load Feast only in a real run, so tests do not need feast installed."""
    from feast import FeatureStore

    repo = os.environ.get("FEAST_REPO_PATH", "feature_store")
    if os.environ.get("FEAST_PUBLICATION_ROOT"):
        return load_published_store(repo)
    return FeatureStore(repo)


async def fetch_features(store, requested_skus):
    """Look up features for a SKU list; lives outside `create_app` so mutmut can mutate it."""
    # Drop duplicate SKUs but keep order: repeated SKUs do not change the result.
    skus = list(dict.fromkeys(requested_skus))
    entity_rows = [{"seller_sku": s} for s in skus]
    results = await asyncio.gather(
        *(
            store.get_online_features_async(
                features=[f"{view}:{name}" for name in names], entity_rows=entity_rows
            )
            for view, names in VIEWS.items()
        ),
        return_exceptions=True,
    )
    if all(isinstance(r, Exception) for r in results):
        raise HTTPException(503, "online store unavailable")
    columns = {"seller_sku": skus}
    for names, result in zip(VIEWS.values(), results, strict=True):
        values = {} if isinstance(result, Exception) else result.to_dict()
        for name in names:
            columns[name] = values.get(name, [None] * len(skus))
    by_view = dict(zip(VIEWS, results, strict=True))
    daily, stream = by_view["sku_return_daily"], by_view["sku_return_24h"]
    release = None if isinstance(daily, Exception) else getattr(daily, "release", None)
    # Legacy/mock OnlineResponse has no real release metadata.
    release = release if isinstance(getattr(release, "release_id", None), str) else None
    rows = []
    for i, sku in enumerate(skus):
        count, positive, rate = (columns[name][i] for name in VIEWS["sku_return_daily"])
        batch_status = (
            "unavailable"
            if isinstance(daily, Exception)
            else daily_status(
                count, positive, rate, expected=bool(release) and sku in release.expected_skus
            )
        )
        stream_status = (
            "unavailable"
            if isinstance(stream, Exception)
            else "not_available"
            if columns["approved_returns_24h"][i] is None
            else "ready"
        )
        rows.append(
            SkuFeatures(
                **{name: values[i] for name, values in columns.items()},
                batch_status=batch_status,
                stream_status=stream_status,
                batch_release=release.release_id if release else None,
                batch_cutoff=release.cutoff if release else None,
            )
        )
    return FeatureResponse(features=rows)


@router.post("/features", response_model=FeatureResponse)
async def features(body: FeatureRequest, request: Request):
    """Look up online features for the SKUs in a REST request."""
    return await fetch_features(request.app.state.store, body.seller_skus)

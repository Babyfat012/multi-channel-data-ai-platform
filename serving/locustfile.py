"""Load test for the feature Web API, HTML output.

locust -f serving/locustfile.py --host http://localhost:8000 --headless -u 20 -r 5 -t 60s \
    --html out/load-test.html
"""

import os
import random

from locust import HttpUser, between, task

SKUS = os.environ.get("LOAD_SKUS", "sku-A,sku-B").split(",")


class FeatureUser(HttpUser):
    """Simulate a user calling single-SKU features, batched features and the health check."""

    wait_time = between(0.05, 0.2)

    @task(4)
    def one_sku(self):
        """Send a feature lookup request for one random SKU."""
        self.client.post("/features", json={"seller_skus": [random.choice(SKUS)]})

    @task(1)
    def batch(self):
        """Send a batched SKU feature lookup request to measure load."""
        self.client.post("/features", json={"seller_skus": SKUS * 5}, name="/features [batch]")

    @task(1)
    def health(self):
        """Send an API health check request as part of the load scenario."""
        self.client.get("/healthz")

from prometheus_client import Counter, Gauge, Histogram

from . import config

request_counter = Counter("herald_num_req", "Total number of requests")

cache_size_bytes = Gauge(
    "herald_cache_size_bytes", "Total size of the cache", labelnames=["type"]
)

cache_item_total = Gauge(
    "herald_cache_item_total", "Total number of items in the cache", labelnames=["type"]
)

cache_cull_total = Counter("herald_cache_cull_total", "Total number of culls")

cache_hits = Counter(
    "herald_cache_hits", "Total number of cache hits", labelnames=["type"]
)
cache_misses = Counter(
    "herald_cache_misses", "Total number of cache misses", labelnames=["type"]
)

cache_read_errors = Counter(
    "herald_cache_read_errors", "Total number of cache read errors", labelnames=["key"]
)

cache_etag_hits = Counter("herald_etag_hits", "Total number of etag hits")

cache_size_max = Gauge(
    "herald_cache_size_max_bytes", "Maximum size of the cache", labelnames=["type"]
)

github_api_call_count = Counter(
    "herald_github_api_call_count", "Number of github api calls", labelnames=["type"]
)

artifact_size = Histogram(
    "herald_artifact_size_bytes",
    "Size of artifacts",
    buckets=[0] + [10**p for p in range(5, 12)] + [float("inf")],
)
artifact_size_rejected = Counter(
    "herald_artifact_size_rejected_total",
    "How often an artifact was rejected due to size",
)

cache_size_max.labels("file").set(config.CACHE_SIZE)
cache_size_max.labels("artifact").set(config.ARTIFACT_CACHE_SIZE)

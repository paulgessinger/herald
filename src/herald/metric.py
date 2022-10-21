from cProfile import label
from prometheus_client import core, Counter, Gauge, CollectorRegistry

request_counter = Counter("herald_num_req", "Total number of requests")

cache_size = Gauge("herald_cache_size_bytes", "Total size of the cache")
cache_hits = Counter(
    "herald_cache_hits", "Total number of cache hits", labelnames=["type"]
)
cache_misses = Counter(
    "herald_cache_misses", "Total number of cache misses", labelnames=["type"]
)

cache_etag_hits = Counter("herald_etag_hits", "Total number of etag hits")

from cProfile import label
from prometheus_client import core, Counter, Gauge, CollectorRegistry, Info

from . import config

request_counter = Counter("herald_num_req", "Total number of requests")

cache_size = Gauge(
    "herald_cache_size_bytes", "Total size of the cache", labelnames=["type"]
)


def get_cache_size():
    from . import github

    gh = github.GitHub()
    return gh._cache.volume() + gh._artifact_cache.total_size()


#  cache_size.set_function(get_cache_size)

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

cache_size_cull_total = Counter("herald_cache_size_cull_total", "Total number of culls")

cache_size_max.labels("file").set(config.CACHE_SIZE)
cache_size_max.labels("artifact").set(config.ARTIFACT_CACHE_SIZE)

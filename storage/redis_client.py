import os
import redis
import time

_redis_client = None

REDIS_BUSY_RETRY_ATTEMPTS = int(os.getenv("REDIS_BUSY_RETRY_ATTEMPTS", "120"))
REDIS_BUSY_RETRY_SLEEP_SEC = float(os.getenv("REDIS_BUSY_RETRY_SLEEP_SEC", "0.5"))


def _is_busy_loading_error(exc: Exception) -> bool:
    return "BusyLoadingError" in str(exc) or "loading the dataset in memory" in str(exc).lower()


def _exec_with_busy_retry(fn):
    last_exc = None
    for _ in range(REDIS_BUSY_RETRY_ATTEMPTS):
        try:
            return fn()
        except Exception as exc:
            if not _is_busy_loading_error(exc):
                raise
            last_exc = exc
            time.sleep(REDIS_BUSY_RETRY_SLEEP_SEC)
    if last_exc:
        raise last_exc


class ResilientRedis(redis.Redis):
    def execute_command(self, *args, **options):
        return _exec_with_busy_retry(
            lambda: super(ResilientRedis, self).execute_command(*args, **options)
        )

    def pipeline(self, transaction=True, shard_hint=None):
        pipe = super(ResilientRedis, self).pipeline(
            transaction=transaction, shard_hint=shard_hint
        )
        original_execute = pipe.execute
        pipe.execute = lambda *args, **kwargs: _exec_with_busy_retry(
            lambda: original_execute(*args, **kwargs)
        )
        return pipe

def get_redis_client():
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        _redis_client = ResilientRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30,
            retry_on_timeout=True,
        )
    return _redis_client

# --- RedisTimeSeries helpers ---
def ts_create_if_not_exists(key, labels=None):
    client = get_redis_client()
    try:
        _exec_with_busy_retry(lambda: client.execute_command("TS.INFO", key))
    except redis.exceptions.ResponseError as e:
        if "does not exist" in str(e):
            cmd = ["TS.CREATE", key, "DUPLICATE_POLICY", "FIRST"]
            if labels:
                cmd.append("LABELS")
                for k, v in labels.items():
                    cmd.extend([k, v])
            _exec_with_busy_retry(lambda: client.execute_command(*cmd))
        elif "unknown command" in str(e):
            raise RuntimeError("RedisTimeSeries module is not loaded.")
        else:
            raise


def ts_add(key, timestamp, value, labels=None, pipe=None , upsert = False):
    """
    Add a value to a RedisTimeSeries key, using DUPLICATE_POLICY=FIRST.
    If `pipe` is provided, the command is added to the pipeline.
    """
    if not hasattr(ts_add, "_initialized_ts_keys"):
        ts_add._initialized_ts_keys = set()
        
    args = []
    if upsert == True:
         args = ["ON_DUPLICATE", "LAST"]
    else:
         args = ["ON_DUPLICATE", "FIRST"]
        
        
    if key not in ts_add._initialized_ts_keys:
        ts_create_if_not_exists(key, labels)
        ts_add._initialized_ts_keys.add(key)
    
    
    client = get_redis_client()
    cmd = ["TS.ADD", key, timestamp, value]  + args
    if pipe:
        pipe.execute_command(*cmd)
    else:
        return _exec_with_busy_retry(lambda: client.execute_command(*cmd))

def mts_range(keys, from_ts, to_ts):
    """
    Get a multi-range of values from RedisTimeSeries.
    keys: list of label filters, e.g. ["symbol=2953217", "timeframe=1m"]
    """
    client = get_redis_client()
    return _exec_with_busy_retry(
        lambda: client.execute_command("TS.MRANGE", from_ts, to_ts, "FILTER", *keys)
    )


def ts_range(key, from_ts, to_ts):
    """
    Get a range of values from a RedisTimeSeries key.
    """
    client = get_redis_client()
    return _exec_with_busy_retry(lambda: client.execute_command("TS.RANGE", key, from_ts, to_ts))

def ts_get(key):
    """
    Get the latest value from a RedisTimeSeries key.
    """
    client = get_redis_client()
    return _exec_with_busy_retry(lambda: client.execute_command("TS.GET", key))

def ts_mrange(from_ts, to_ts, filters):
    """
    Fetch multiple time series with MRANGE.
    filters: list of label filters, e.g. ["symbol=2953217", "timeframe=1m"]
    Returns: list of series, each as [key, labels, [ [timestamp, value], ... ]]
    """
    client = get_redis_client()
    print("mrange_query", "TS.MREVRANGE", from_ts, to_ts, "FILTER", *filters)
    return _exec_with_busy_retry(
        lambda: client.execute_command("TS.MREVRANGE", from_ts, to_ts, "FILTER", *filters)
    )

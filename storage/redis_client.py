import os
import redis

_redis_client = None

def get_redis_client():
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        _redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
    return _redis_client

# --- RedisTimeSeries helpers ---
def ts_create_if_not_exists(key, labels=None):
    client = get_redis_client()
    try:
        client.execute_command("TS.INFO", key)
    except redis.exceptions.ResponseError as e:
        if "does not exist" in str(e):
            cmd = ["TS.CREATE", key, "DUPLICATE_POLICY", "FIRST"]
            if labels:
                cmd.append("LABELS")
                for k, v in labels.items():
                    cmd.extend([k, v])
            client.execute_command(*cmd)
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
        
    if key not in ts_add._initialized_ts_keys:
        ts_create_if_not_exists(key, labels)
        ts_add._initialized_ts_keys.add(key)
    
    
    client = get_redis_client()
    cmd = ["TS.ADD", key, timestamp, value]  + args
    if pipe:
        pipe.execute_command(*cmd)
    else:
        return client.execute_command(*cmd)


def ts_range(key, from_ts, to_ts):
    """
    Get a range of values from a RedisTimeSeries key.
    """
    client = get_redis_client()
    return client.execute_command("TS.RANGE", key, from_ts, to_ts)

def ts_get(key):
    """
    Get the latest value from a RedisTimeSeries key.
    """
    client = get_redis_client()
    return client.execute_command("TS.GET", key)

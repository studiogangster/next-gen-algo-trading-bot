from typing import Optional
import typer
import yaml
import os
import re
from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from dotenv import load_dotenv
import time
from core.order_sync_service import run_order_sync_loop
from core.eod_squareoff_service import run_eod_squareoff_loop

app = typer.Typer()

# Load .env file if present
load_dotenv()

def load_raw_config(path: str) -> dict:
    """
    Loads YAML config and substitutes ${VAR} with environment variables.
    """
    pattern = re.compile(r"\$\{(\w+)\}")

    def env_var_constructor(loader, node):
        value = loader.construct_scalar(node)
        def replace_var(match):
            var_name = match.group(1)
            if var_name in os.environ:
                return os.environ[var_name]
            raise ValueError(f"Environment variable '{var_name}' not set")
        return pattern.sub(replace_var, value)

    yaml.SafeLoader.add_implicit_resolver('!env_var', pattern, None)
    yaml.SafeLoader.add_constructor('!env_var', env_var_constructor)

    def env_var_hook(loader, node):
        return env_var_constructor(loader, node)

    yaml.SafeLoader.add_constructor('tag:yaml.org,2002:str', env_var_hook)

    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data or {}


def apply_broker_env_from_config(path: str) -> dict:
    """
    Load config and hydrate env vars used by broker login helpers.
    """
    raw = load_raw_config(path)
    broker = raw.get("broker", {}) if isinstance(raw, dict) else {}
    feed = raw.get("feed", {}) if isinstance(raw, dict) else {}

    if isinstance(broker, dict):
        if broker.get("username") is not None:
            os.environ["USERID"] = str(broker.get("username"))
        if broker.get("password") is not None:
            os.environ["PASSWORD"] = str(broker.get("password"))
        if broker.get("otp_salt") is not None:
            os.environ["OTP_SALT"] = str(broker.get("otp_salt"))
    if isinstance(feed, dict) and feed.get("api_key") is not None:
        os.environ["ZERODHA_API_KEY"] = str(feed.get("api_key"))
    return raw


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default

@app.command()
def start(config_path: str = typer.Option("config/config.yaml", help="Path to config YAML file")):
    """
    Start trading engine with the given config.
    """
    from feeds.zerodha_ws import ZerodhaWebSocketFeed
    from storage.parquet import ParquetStorage
    from strategies.supertrend_rsi import SupertrendRSIStrategy
    from strategies.yaml_rule import YamlRuleStrategy
    from core.engine import Engine, EngineConfig
    from config.settings import Settings

    raw_settings = apply_broker_env_from_config(config_path)
    settings = Settings(**raw_settings)


    # Instantiate feed
    if settings.feed.type == "zerodha_ws":
        feed_config = settings.feed
        login_response = login(
            username=feed_config.username,
            password=feed_config.password,
            otp_salt=feed_config.otp_salt,
        )
        enctoken = login_response["enctoken"]     
        
        access_token = enctoken+"&user_id="+feed_config.username   
        feed = ZerodhaWebSocketFeed(
            api_key=feed_config.api_key,
            access_token=access_token,
        )
    else:
        raise NotImplementedError(f"Feed type {settings.feed.type} not implemented")

    # Instantiate broker
    if settings.broker.type == "zerodha":
        login_response = login(
            username=settings.broker.username,
            password=settings.broker.password,
            otp_salt=settings.broker.otp_salt,
        )
        enctoken = login_response["enctoken"]     
        broker = ZerodhaBroker(


        )
    else:
        raise NotImplementedError(f"Broker type {settings.broker.type} not implemented")

    # Instantiate storage
    if settings.storage.type == "parquet":
        storage = ParquetStorage(base_dir=settings.storage.base_dir)
    else:
        raise NotImplementedError(f"Storage type {settings.storage.type} not implemented")

    # Instantiate strategies
    strategies = []
    for strat_cfg in settings.strategies:
        if strat_cfg.type == "supertrend_rsi":
            strategies.append(SupertrendRSIStrategy(**strat_cfg.params))
        elif strat_cfg.type in {"yaml_rule", "rule_based", "yaml_signal"}:
            strategies.append(YamlRuleStrategy(**strat_cfg.params))
        else:
            raise NotImplementedError(f"Strategy {strat_cfg.type} not implemented")




    # Engine config
    max_workers = env_int("ENGINE_MAX_WORKERS", settings.max_workers)
    derived_timeframes = sorted(set(tf for tf in settings.derived_timeframes if tf not in set(settings.timeframes)))

    engine_config = EngineConfig(
        symbols=settings.symbols,
        timeframes=settings.timeframes,
        derived_timeframes=derived_timeframes,
        strategies=strategies,
        feed=feed,
        broker=broker,
        storage=storage,
        dry_run=settings.dry_run,
        max_workers=max_workers,
        bootstrap_universe=env_bool("ENGINE_BOOTSTRAP_UNIVERSE", True),
        universe_instruments_csv="data/reference/instruments.csv.gz",
        universe_index_dir="data/reference/indexes",
        universe_namespace="universe:v1",
        universe_refresh_hours=20,
        universe_sync_default_indexes=True,
        indicators=settings.indicators,
        indicator_timeframes=sorted(set(settings.timeframes + settings.derived_timeframes)),
        indicator_poll_interval=1.0,
        timeframe_generator_poll_interval=env_float("ENGINE_TIMEFRAME_SYNC_POLL_INTERVAL", 5.0),
        timeframe_max_1m_points_per_cycle=env_int("ENGINE_TIMEFRAME_MAX_1M_POINTS_PER_CYCLE", 1200),
        enable_order_sync_worker=env_bool("ENGINE_ENABLE_ORDER_SYNC", True),
        enable_symbol_workers=env_bool("ENGINE_ENABLE_SYMBOL_SYNC", True),
        enable_indicator_worker=env_bool("ENGINE_ENABLE_INDICATOR_SYNC", True),
        enable_timeframe_worker=env_bool("ENGINE_ENABLE_TIMEFRAME_SYNC", True),
    )
    
    


    engine = Engine(engine_config)
    
    
    
    typer.echo("Starting trading engine...")
    engine.start()
    typer.echo("Engine running. Press Ctrl+C to stop.")
    # position_order_engine = OrderAndPositionWorker(broker=broker)
    # typer.echo("Starting local-broker engine...")
    # position_order_engine.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        typer.echo("Shutting down engine...")
        engine.stop()

@app.command()
def stop():
    """
    Stop the trading engine (if running).
    """
    # In this simple CLI, stop is a placeholder.
    typer.echo("To stop the engine, interrupt the process (Ctrl+C).")


@app.command("start-order-sync")
def start_order_sync(
    config_path: str = typer.Option("config/config.yaml", help="Path to config YAML file"),
    poll_interval: float = typer.Option(0.5, help="Polling interval in seconds."),
):
    """
    Start dedicated broker->Redis order/position sync service.
    """
    apply_broker_env_from_config(config_path)
    typer.echo(f"Starting order sync service (poll={poll_interval}s)...")
    run_order_sync_loop(poll_interval=poll_interval)


@app.command("start-eod-squareoff")
def start_eod_squareoff(
    config_path: str = typer.Option("config/config.yaml", help="Path to config YAML file"),
    trigger_time_ist: str = typer.Option("15:20", help="IST trigger time in HH:MM format."),
    poll_interval: float = typer.Option(15.0, help="Polling interval in seconds."),
    dry_run: bool = typer.Option(False, help="If true, only logs planned exits without placing orders."),
    cancel_pending_orders: bool = typer.Option(True, help="Cancel pending orders before square-off."),
    weekdays_only: bool = typer.Option(True, help="Run only Monday-Friday in IST."),
):
    """
    Start dedicated end-of-day auto square-off service.
    """
    apply_broker_env_from_config(config_path)
    typer.echo(
        "Starting EOD square-off service "
        f"(trigger={trigger_time_ist} IST, poll={poll_interval}s, dry_run={dry_run})..."
    )
    run_eod_squareoff_loop(
        trigger_time_ist=trigger_time_ist,
        poll_interval=poll_interval,
        dry_run=dry_run,
        cancel_pending_orders=cancel_pending_orders,
        weekdays_only=weekdays_only,
    )

@app.command('generate-timeframe')
def generate_timeframes(
    config_path: str = typer.Option("config/config.yaml", help="Path to config YAML file"),
    poll_interval: int = typer.Option(5, help="Polling interval in seconds")
):
    """
    Start the timeframe generator worker to aggregate 1m candles into higher timeframes.
    """
    import ray
    from core.timeframe_generator_worker import TimeframeGeneratorWorker
    from config.settings import Settings

    typer.echo("Starting TimeframeGeneratorWorker...")
    ray.init(ignore_reinit_error=True)
    
    
    worker = TimeframeGeneratorWorker.remote(
        config=Settings(**load_raw_config(config_path)),
        poll_interval=poll_interval,
    )
    worker.run.remote()
    typer.echo("TimeframeGeneratorWorker running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        typer.echo("Shutting down TimeframeGeneratorWorker...")


@app.command("sync-instruments")
def sync_instruments(
    output_path: str = typer.Option(
        "data/reference/instruments.csv.gz",
        help="Output CSV/CSV.GZ path for instruments master dump.",
    ),
    exchange: Optional[str] = typer.Option(
        None,
        help="Optional exchange filter at source (e.g. NSE, NFO, BSE). If not set, fetches all exchanges.",
    ),
    config_path: str = typer.Option(
        "config/config.yaml",
        help="Path to config YAML file (used to populate broker credentials).",
    ),
):
    """
    Fetch and save instruments master dump from Kite.
    """
    from brokers.instrument_master import fetch_and_save_instruments

    apply_broker_env_from_config(config_path)
    saved_path, row_count = fetch_and_save_instruments(output_path=output_path, exchange=exchange)
    typer.echo(f"Saved instruments dump to: {saved_path}")
    typer.echo(f"Rows: {row_count}")

@app.command("sync-indexes")
def sync_indexes(
    output_dir: str = typer.Option(
        "data/reference/indexes",
        help="Directory to save downloaded index constituent CSVs.",
    ),
):
    """
    Download default index constituent files.
    """
    from brokers.instrument_master import sync_all_nse_index_constituents

    paths = sync_all_nse_index_constituents(output_dir=output_dir)
    typer.echo(f"Downloaded {len(paths)} index files into: {output_dir}")
    for p in paths:
        typer.echo(f"- {p}")


@app.command("enrich-index")
def enrich_index(
    constituents_csv: str = typer.Option(..., help="Path to index constituents CSV (must include symbol column)."),
    instruments_csv: str = typer.Option(
        "data/reference/instruments.csv.gz",
        help="Path to instruments master CSV/CSV.GZ generated from sync-instruments.",
    ),
    output_csv: str = typer.Option(
        "data/reference/index_enriched.csv",
        help="Output CSV/CSV.GZ path for enriched index symbols.",
    ),
    index_name: Optional[str] = typer.Option(None, help="Optional index name label to store in output."),
    symbol_column: Optional[str] = typer.Option(
        None,
        help="Explicit symbol column in constituents CSV. If omitted, auto-detection is used.",
    ),
    exchange: str = typer.Option("NSE", help="Exchange filter to map symbols."),
    instrument_type: str = typer.Option("EQ", help="Instrument type filter to map symbols."),
):
    """
    Enrich index constituents with instrument metadata and tokens.
    """
    from brokers.instrument_master import enrich_index_constituents

    summary = enrich_index_constituents(
        constituents_csv_path=constituents_csv,
        instruments_csv_path=instruments_csv,
        output_path=output_csv,
        index_name=index_name,
        symbol_column=symbol_column,
        exchange=exchange,
        instrument_type=instrument_type,
    )

    typer.echo(f"Saved enriched output to: {summary['output_path']}")
    typer.echo(
        f"Mapped {summary['mapped_symbols']}/{summary['total_symbols']} "
        f"(unmapped: {summary['unmapped_symbols']})"
    )
    typer.echo(f"Symbol column used: {summary['symbol_column_used']}")


@app.command("ingest-universe")
def ingest_universe(
    instruments_csv: str = typer.Option(
        "data/reference/instruments.csv.gz",
        help="Path to instruments master CSV/CSV.GZ.",
    ),
    index_dir: str = typer.Option(
        "data/reference/indexes",
        help="Directory containing index constituent CSV files.",
    ),
    namespace: str = typer.Option(
        "universe:v1",
        help="Redis keyspace namespace for universe catalog.",
    ),
):
    """
    Ingest instruments + index constituents into Redis for UI discovery.
    """
    from brokers.instrument_master import ingest_universe_to_redis, discover_index_constituent_files

    index_files = discover_index_constituent_files(index_dir=index_dir)
    summary = ingest_universe_to_redis(
        instruments_csv_path=instruments_csv,
        index_files=index_files,
        namespace=namespace,
    )
    typer.echo(f"Universe ingested in namespace: {summary['namespace']}")
    typer.echo(f"Instruments: {summary['instruments_count']}")
    typer.echo(
        f"Indexes: {summary['indexes_count']} "
        f"(symbols: {summary['index_symbols_count']}, mapped: {summary['index_mapped_count']})"
    )
    typer.echo(f"Deleted old namespace keys: {summary['deleted_keys']}")

if __name__ == "__main__":
    app()

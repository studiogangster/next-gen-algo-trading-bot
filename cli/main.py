from typing import cast
import typer
import yaml
import os
import re
from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from dotenv import load_dotenv
from brokers.zerodha import sync_zerodha_historical_realtime
from config.settings import BrokerConfig, FeedConfig, Settings
from core.engine import  Engine, EngineConfig
from strategies.supertrend_rsi import SupertrendRSIStrategy
from feeds.zerodha_ws import ZerodhaWebSocketFeed
from storage.parquet import ParquetStorage
import time

app = typer.Typer()

# Load .env file if present
load_dotenv()

def load_config(path: str) -> Settings:
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
    return Settings(**data)

@app.command()
def start(config_path: str = typer.Option("config/config.yaml", help="Path to config YAML file")):
    """
    Start trading engine with the given config.
    """
    
    settings = load_config(config_path)


    # Instantiate feed
    if settings.feed.type == "zerodha_ws":
        
        feed_config = cast(FeedConfig, settings.feed)
        login_response =  login(   )
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
        login_response =  login(    )
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
        else:
            raise NotImplementedError(f"Strategy {strat_cfg.type} not implemented")




    # Engine config
    engine_config = EngineConfig(
        symbols=settings.symbols,
        timeframes=settings.timeframes,
        strategies=strategies,
        feed=feed,
        broker=broker,
        storage=storage,
        dry_run=settings.dry_run,
        max_workers=settings.max_workers,
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

if __name__ == "__main__":
    app()

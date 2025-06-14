from pydantic import BaseModel, Field, validator
from typing import List, Literal, Optional

class FeedConfig(BaseModel):
    type: Literal["zerodha_ws", "rest", "kafka"]
    
    username: str
    password: str
    otp_salt: str
    api_key: str


    # api_key: Optional[str]
    # access_token: Optional[str]
    # Add more fields as needed for other feed types

class BrokerConfig(BaseModel):
    type: Literal["zerodha"]
    username: str
    password: str
    otp_salt: str
    api_key: str


# class BrokerConfig(BaseModel):
#     type: Literal["zerodha_api"]
#     api_key: str
#     access_token: str
    
class StorageConfig(BaseModel):
    type: Literal["parquet", "postgres", "timescaledb"]
    base_dir: Optional[str] = "data"
    dsn: Optional[str] = None  # For postgres/timescaledb

class StrategyConfig(BaseModel):
    type: str  # e.g., "supertrend_rsi"
    params: dict = Field(default_factory=dict)

class Settings(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    feed: FeedConfig
    broker: BrokerConfig
    storage: StorageConfig
    strategies: List[StrategyConfig]
    dry_run: bool = False
    max_workers: int = 4

    @validator("timeframes", pre=True)
    def validate_timeframes(cls, v):
        allowed = {"1m", "5m", "30m", "1h"}
        for tf in v:
            if tf not in allowed:
                raise ValueError(f"Unsupported timeframe: {tf}")
        return v

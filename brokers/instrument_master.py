from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests

from brokers.kite_trade import ZerodhaBroker
from storage.redis_client import get_redis_client


EXPECTED_INSTRUMENT_COLUMNS = [
    "instrument_token",
    "exchange_token",
    "tradingsymbol",
    "name",
    "last_price",
    "expiry",
    "strike",
    "tick_size",
    "lot_size",
    "instrument_type",
    "segment",
    "exchange",
]

SYMBOL_COLUMN_CANDIDATES = [
    "symbol",
    "Symbol",
    "SYMBOL",
    "tradingsymbol",
    "TradingSymbol",
    "TRADINGSYMBOL",
]

DEFAULT_INDEX_SOURCES = {
    "NIFTY50": "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "NIFTYBANK": "https://nsearchives.nseindia.com/content/indices/ind_niftybanklist.csv",
    "NIFTYFINSERVICE": "https://nsearchives.nseindia.com/content/indices/ind_niftyfinservice50list.csv",
    "NIFTYNEXT50": "https://nsearchives.nseindia.com/content/indices/ind_niftynext50list.csv",
}

NSE_INDEX_QUERY_NAMES = {
    "NIFTY50": "NIFTY 50",
    "NIFTYBANK": "NIFTY BANK",
    "NIFTYNEXT50": "NIFTY NEXT 50",
}

DEFAULT_UNIVERSE_NAMESPACE = "universe:v1"


def _instruments_url() -> str:
    return "https://api.kite.trade/instruments"


def fetch_instruments_dataframe(
    exchange: Optional[str] = None,
    broker: Optional[ZerodhaBroker] = None,
    timeout_seconds: int = 60,
) -> pd.DataFrame:
    """
    Fetch instruments dump from Kite and return as DataFrame.
    If exchange is provided, uses /instruments/{exchange}; else /instruments.
    """
    client = broker or ZerodhaBroker()
    response = client.session.get(
        _instruments_url(),
        headers=client.headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    df = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    missing = [c for c in EXPECTED_INSTRUMENT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected instrument columns: {missing}")

    if exchange:
        df = df[df["exchange"].astype(str).str.upper() == exchange.upper()].copy()

    return df


def save_instruments_dataframe(df: pd.DataFrame, output_path: str) -> Path:
    """
    Save instruments DataFrame to CSV (.csv or .csv.gz).
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    compression = "gzip" if out.suffix == ".gz" else None
    df.to_csv(out, index=False, compression=compression)
    return out


def fetch_and_save_instruments(
    output_path: str = "data/reference/instruments.csv.gz",
    exchange: Optional[str] = None,
) -> tuple[Path, int]:
    """
    Fetch instruments from Kite and save to disk.
    Returns (saved_path, row_count).
    """
    df = fetch_instruments_dataframe(exchange=exchange)
    saved = save_instruments_dataframe(df, output_path)
    return saved, len(df)


def _resolve_symbol_column(df: pd.DataFrame, explicit_symbol_column: Optional[str] = None) -> str:
    if explicit_symbol_column:
        if explicit_symbol_column not in df.columns:
            raise ValueError(
                f"Provided symbol column '{explicit_symbol_column}' not found. "
                f"Available columns: {list(df.columns)}"
            )
        return explicit_symbol_column

    for candidate in SYMBOL_COLUMN_CANDIDATES:
        if candidate in df.columns:
            return candidate

    raise ValueError(
        "Could not find symbol column in index constituents file. "
        f"Tried: {SYMBOL_COLUMN_CANDIDATES}. Available columns: {list(df.columns)}"
    )

def _normalize_index_name(raw: str) -> str:
    return "".join(ch for ch in raw.upper().replace(" ", "_") if ch.isalnum() or ch == "_")


def _nse_session(timeout_seconds: int = 30) -> requests.Session:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    }
    session.get("https://www.nseindia.com", headers=headers, timeout=timeout_seconds)
    return session


def sync_index_constituents(
    index_sources: Dict[str, str],
    output_dir: str = "data/reference/indexes",
    timeout_seconds: int = 30,
) -> List[Path]:
    """
    Download index constituent CSV files from provided sources.
    Returns saved file paths.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*;q=0.8"}

    for raw_name, url in index_sources.items():
        index_name = _normalize_index_name(raw_name)
        out_path = out_dir / f"{index_name}.csv"
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_seconds)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            saved.append(out_path)
        except Exception:
            # Continue with other indexes; callers can inspect saved list.
            continue

    return saved


def sync_default_index_constituents(output_dir: str = "data/reference/indexes") -> List[Path]:
    """
    Download a default set of commonly used NSE index constituent files.
    """
    return sync_index_constituents(DEFAULT_INDEX_SOURCES, output_dir=output_dir)


def fetch_nse_all_index_names(timeout_seconds: int = 30) -> List[str]:
    """
    Fetch all index names from NSE allIndices endpoint.
    """
    session = _nse_session(timeout_seconds=timeout_seconds)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    }
    resp = session.get("https://www.nseindia.com/api/allIndices", headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    payload = resp.json()
    names = []
    for row in payload.get("data", []):
        name = str(row.get("index") or "").strip()
        if name:
            names.append(name)
    # Stable unique order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out


def discover_index_constituent_files(index_dir: str = "data/reference/indexes") -> List[Path]:
    """
    Return all CSV files under index_dir (non-recursive), sorted by name.
    """
    root = Path(index_dir)
    if not root.exists():
        return []
    return sorted(p for p in root.glob("*.csv") if p.is_file())


def fetch_nse_live_index_weights(index_query_name: str, timeout_seconds: int = 30) -> dict:
    """
    Fetch live index constituents from NSE and derive dynamic weight % from FFMC.
    Returns dict with `asof` and `weights_by_symbol`.
    """
    session = _nse_session(timeout_seconds=timeout_seconds)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    }
    url = f"https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(index_query_name)}"
    resp = session.get(url, headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("data", [])
    asof = payload.get("timestamp") or payload.get("name") or ""

    constituents = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        identifier = str(row.get("identifier") or "").strip().upper()
        if not symbol:
            continue
        if symbol == index_query_name.upper() or identifier == index_query_name.upper():
            continue
        ffmc_raw = row.get("ffmc")
        try:
            ffmc = float(ffmc_raw) if ffmc_raw not in (None, "") else 0.0
        except Exception:
            ffmc = 0.0
        constituents.append(
            {
                "symbol": symbol,
                "ffmc": ffmc,
                "last_price": row.get("lastPrice"),
                "pchange": row.get("pChange"),
            }
        )

    total_ffmc = sum(max(0.0, c["ffmc"]) for c in constituents)
    weights = {}
    if total_ffmc > 0:
        for c in constituents:
            weight_pct = (max(0.0, c["ffmc"]) / total_ffmc) * 100.0
            weights[c["symbol"]] = {
                "weight_pct": weight_pct,
                "ffmc": c["ffmc"],
                "last_price": c["last_price"],
                "pchange": c["pchange"],
            }

    return {
        "asof": asof,
        "weights_by_symbol": weights,
        "count": len(weights),
    }


def sync_all_nse_index_constituents(
    output_dir: str = "data/reference/indexes",
    timeout_seconds: int = 30,
    max_indexes: Optional[int] = None,
) -> List[Path]:
    """
    Discover index universe from NSE and persist fetchable constituent snapshots.
    Produces CSVs with Symbol + computed WeightPct (from FFMC snapshot where available).
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []

    names = fetch_nse_all_index_names(timeout_seconds=timeout_seconds)
    if max_indexes is not None and max_indexes > 0:
        names = names[:max_indexes]

    for name in names:
        try:
            payload = fetch_nse_live_index_weights(name, timeout_seconds=timeout_seconds)
        except Exception:
            continue

        weights = payload.get("weights_by_symbol", {})
        if not weights:
            continue

        rows = []
        for symbol, info in sorted(weights.items()):
            rows.append(
                {
                    "Symbol": symbol,
                    "WeightPct": info.get("weight_pct"),
                    "FFMC": info.get("ffmc"),
                    "LastPrice": info.get("last_price"),
                    "PChange": info.get("pchange"),
                }
            )
        df = pd.DataFrame(rows)
        idx_name = _normalize_index_name(name)
        out_path = out_dir / f"{idx_name}.csv"
        df.to_csv(out_path, index=False)
        saved.append(out_path)

    # Safety net: keep old default source sync too (if any missing from dynamic pull)
    for p in sync_default_index_constituents(output_dir=output_dir):
        if p not in saved:
            saved.append(p)

    return sorted(saved)


def enrich_index_constituents(
    constituents_csv_path: str,
    instruments_csv_path: str,
    output_path: str,
    index_name: Optional[str] = None,
    symbol_column: Optional[str] = None,
    exchange: str = "NSE",
    instrument_type: str = "EQ",
) -> dict:
    """
    Join index constituent symbols with instruments master and emit enriched CSV.
    Returns summary stats.
    """
    constituents = pd.read_csv(constituents_csv_path, low_memory=False)
    instruments = pd.read_csv(instruments_csv_path, low_memory=False)

    sym_col = _resolve_symbol_column(constituents, explicit_symbol_column=symbol_column)
    constituents = constituents.copy()
    constituents["_symbol_norm"] = constituents[sym_col].astype(str).str.strip().str.upper()

    enriched_instruments = instruments.copy()
    enriched_instruments = enriched_instruments[
        (enriched_instruments["exchange"].astype(str).str.upper() == exchange.upper())
        & (enriched_instruments["instrument_type"].astype(str).str.upper() == instrument_type.upper())
    ]
    enriched_instruments["_symbol_norm"] = (
        enriched_instruments["tradingsymbol"].astype(str).str.strip().str.upper()
    )

    keep_cols = [
        "_symbol_norm",
        "instrument_token",
        "exchange_token",
        "tradingsymbol",
        "name",
        "last_price",
        "expiry",
        "strike",
        "tick_size",
        "lot_size",
        "instrument_type",
        "segment",
        "exchange",
    ]
    enriched_instruments = enriched_instruments[keep_cols].drop_duplicates(subset=["_symbol_norm"], keep="first")

    joined = constituents.merge(enriched_instruments, on="_symbol_norm", how="left")
    joined["index_name"] = index_name or Path(constituents_csv_path).stem
    joined["is_mapped"] = joined["instrument_token"].notna()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    compression = "gzip" if output.suffix == ".gz" else None
    joined.to_csv(output, index=False, compression=compression)

    total = len(joined)
    mapped = int(joined["is_mapped"].sum())
    unmapped = total - mapped
    return {
        "output_path": str(output),
        "total_symbols": total,
        "mapped_symbols": mapped,
        "unmapped_symbols": unmapped,
        "symbol_column_used": sym_col,
        "exchange_filter": exchange,
        "instrument_type_filter": instrument_type,
    }


def _serialize_record(record: dict) -> dict:
    out = {}
    for k, v in record.items():
        if pd.isna(v):
            out[k] = ""
        else:
            out[k] = str(v)
    return out


def _delete_namespace(client, namespace: str) -> int:
    cursor = 0
    deleted = 0
    pattern = f"{namespace}:*"
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=1000)
        if keys:
            client.delete(*keys)
            deleted += len(keys)
        if cursor == 0:
            break
    return deleted


def ingest_universe_to_redis(
    instruments_csv_path: str,
    index_files: Sequence[str | Path] = (),
    namespace: str = DEFAULT_UNIVERSE_NAMESPACE,
) -> dict:
    """
    Ingest instrument master and index constituent mappings into Redis.
    Designed for UI discovery and future-proof filtering.
    """
    client = get_redis_client()
    instruments = pd.read_csv(instruments_csv_path, low_memory=False)
    missing = [c for c in EXPECTED_INSTRUMENT_COLUMNS if c not in instruments.columns]
    if missing:
        raise ValueError(f"Missing expected instrument columns in instruments CSV: {missing}")

    deleted_keys = _delete_namespace(client, namespace)

    all_set = f"{namespace}:instruments:all"
    tokens_zset = f"{namespace}:instruments:tokens"
    now_iso = datetime.now(timezone.utc).isoformat()

    symbol_to_token_nse_eq: Dict[str, str] = {}
    batch = client.pipeline(transaction=False)
    batch_size = 0
    for row in instruments.to_dict(orient="records"):
        rec = _serialize_record(row)
        token = rec.get("instrument_token", "").strip()
        if not token:
            continue
        exchange = rec.get("exchange", "").upper()
        instrument_type = rec.get("instrument_type", "").upper()
        segment = rec.get("segment", "").upper()
        tradingsymbol = rec.get("tradingsymbol", "").upper()

        if exchange == "NSE" and instrument_type == "EQ" and tradingsymbol:
            symbol_to_token_nse_eq[tradingsymbol] = token

        instrument_key = f"{namespace}:instrument:{token}"
        batch.hset(instrument_key, mapping=rec)
        batch.sadd(all_set, token)
        batch.zadd(tokens_zset, {token: int(float(token))})
        batch.sadd(f"{namespace}:instruments:exchange:{exchange}", token)
        batch.sadd(f"{namespace}:instruments:type:{instrument_type}", token)
        batch.sadd(f"{namespace}:instruments:segment:{segment}", token)
        batch.set(f"{namespace}:symbol:{exchange}:{tradingsymbol}", token)

        batch_size += 1
        if batch_size >= 1000:
            batch.execute()
            batch = client.pipeline(transaction=False)
            batch_size = 0

    if batch_size:
        batch.execute()

    index_count = 0
    total_index_symbols = 0
    total_index_mapped = 0
    for p in index_files:
        path = Path(p)
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)
        sym_col = _resolve_symbol_column(df)
        index_name = _normalize_index_name(path.stem)
        symbols = (
            df[sym_col]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace("NAN", "")
            .tolist()
        )
        symbols = [s for s in symbols if s]
        if not symbols:
            continue

        weights_payload = None
        weights_by_symbol = {}
        # Prefer snapshot weights already present in CSV files generated by sync_all_nse_index_constituents.
        if "WeightPct" in df.columns:
            for _, row in df.iterrows():
                symbol = str(row.get(sym_col, "")).strip().upper()
                if not symbol:
                    continue
                try:
                    weight_pct = float(row.get("WeightPct")) if row.get("WeightPct") not in ("", None) else None
                except Exception:
                    weight_pct = None
                try:
                    ffmc_value = float(row.get("FFMC")) if row.get("FFMC") not in ("", None) else None
                except Exception:
                    ffmc_value = None
                if weight_pct is not None:
                    weights_by_symbol[symbol] = {
                        "weight_pct": weight_pct,
                        "ffmc": ffmc_value,
                        "last_price": row.get("LastPrice"),
                        "pchange": row.get("PChange"),
                    }
            if weights_by_symbol:
                weights_payload = {"asof": now_iso, "weights_by_symbol": weights_by_symbol, "count": len(weights_by_symbol)}
        # Fallback: live pull for a few known indexes
        if not weights_by_symbol:
            query_name = NSE_INDEX_QUERY_NAMES.get(index_name)
            if query_name:
                try:
                    weights_payload = fetch_nse_live_index_weights(query_name)
                    weights_by_symbol = weights_payload.get("weights_by_symbol", {})
                except Exception:
                    weights_payload = None
                    weights_by_symbol = {}

        pipe = client.pipeline(transaction=False)
        mapped = 0
        weighted = 0
        weight_key = f"{namespace}:index:{index_name}:weight_pct"
        ffmc_key = f"{namespace}:index:{index_name}:ffmc"
        last_price_key = f"{namespace}:index:{index_name}:last_price"
        pchange_key = f"{namespace}:index:{index_name}:pchange"
        for symbol in symbols:
            pipe.sadd(f"{namespace}:index:{index_name}:symbols", symbol)
            pipe.sadd(f"{namespace}:symbol:{symbol}:indexes", index_name)
            token = symbol_to_token_nse_eq.get(symbol)
            if token:
                mapped += 1
                pipe.sadd(f"{namespace}:index:{index_name}:tokens", token)
            weight_info = weights_by_symbol.get(symbol)
            if weight_info:
                weighted += 1
                pipe.hset(weight_key, symbol, str(weight_info.get("weight_pct", "")))
                pipe.hset(ffmc_key, symbol, str(weight_info.get("ffmc", "")))
                pipe.hset(last_price_key, symbol, str(weight_info.get("last_price", "")))
                pipe.hset(pchange_key, symbol, str(weight_info.get("pchange", "")))
        pipe.sadd(f"{namespace}:indexes", index_name)
        index_meta = {
            "index_name": index_name,
            "source_file": str(path),
            "symbol_column": sym_col,
            "symbols_count": str(len(symbols)),
            "mapped_count": str(mapped),
            "updated_at": now_iso,
            "weighted_symbols_count": str(weighted),
            "weight_mode": "snapshot_weight_pct" if "WeightPct" in df.columns else ("dynamic_ffmc" if weights_by_symbol else "unavailable"),
            "weight_source": "nse_equity_stockIndices",
            "weight_asof": (weights_payload or {}).get("asof", ""),
        }
        pipe.hset(
            f"{namespace}:index:{index_name}:meta",
            mapping=index_meta,
        )
        pipe.execute()
        index_count += 1
        total_index_symbols += len(symbols)
        total_index_mapped += mapped

    client.hset(
        f"{namespace}:meta",
        mapping={
            "version": "1",
            "ingested_at": now_iso,
            "instruments_count": str(len(instruments)),
            "indexes_count": str(index_count),
            "index_symbols_count": str(total_index_symbols),
            "index_mapped_count": str(total_index_mapped),
        },
    )

    return {
        "namespace": namespace,
        "deleted_keys": deleted_keys,
        "instruments_count": len(instruments),
        "indexes_count": index_count,
        "index_symbols_count": total_index_symbols,
        "index_mapped_count": total_index_mapped,
        "ingested_at": now_iso,
    }


def ensure_universe_catalog(
    instruments_csv_path: str = "data/reference/instruments.csv.gz",
    index_dir: str = "data/reference/indexes",
    namespace: str = DEFAULT_UNIVERSE_NAMESPACE,
    refresh_if_older_than_hours: int = 20,
    sync_default_indexes: bool = True,
) -> dict:
    """
    Ensure Redis universe catalog is available and fresh enough.
    - Fetches instruments CSV if missing.
    - Optionally syncs default index constituent CSVs.
    - Ingests everything into Redis namespace.
    """
    client = get_redis_client()
    meta_key = f"{namespace}:meta"
    meta = client.hgetall(meta_key) or {}
    last_ingested = meta.get("ingested_at")
    if last_ingested:
        try:
            last_dt = datetime.fromisoformat(last_ingested.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - last_dt
            if age < timedelta(hours=refresh_if_older_than_hours):
                return {"status": "skipped", "reason": "fresh_catalog", "meta": meta}
        except Exception:
            pass

    instruments_path = Path(instruments_csv_path)
    if not instruments_path.exists():
        fetch_and_save_instruments(output_path=str(instruments_path))

    if sync_default_indexes:
        try:
            sync_all_nse_index_constituents(output_dir=index_dir)
        except Exception:
            # Allow ingestion even if some index downloads fail.
            pass

    index_files = discover_index_constituent_files(index_dir=index_dir)
    summary = ingest_universe_to_redis(
        instruments_csv_path=str(instruments_path),
        index_files=index_files,
        namespace=namespace,
    )
    summary["status"] = "ingested"
    summary["index_files"] = [str(p) for p in index_files]
    return summary

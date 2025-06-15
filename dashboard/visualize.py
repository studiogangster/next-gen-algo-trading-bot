import streamlit as st
import pandas as pd
import os
import plotly.graph_objs as go
import pytz

st.set_page_config(page_title="Real-Time Aggregator Visualization", layout="wide")
st.title("Real-Time Aggregator Visualization")

# --- User Inputs ---
symbol = st.text_input("Symbol", value="256265")
timeframe = st.selectbox("Timeframe", options=["1m", "5m", "15m", "30m", "1h", "day"], index=0)

st.info(
    "This dashboard visualizes real-time candles for the selected symbol and timeframe. "
    "Ensure your trading engine is writing tick data to 'data/aggregator_<symbol>.parquet'."
)

plot_placeholder = st.empty()
table_placeholder = st.empty()


def load_and_resample(symbol, timeframe):
    parquet_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", f"aggregator_{symbol}.parquet")
    if not os.path.exists(parquet_path):
        st.warning(f"No data file found for symbol '{symbol}'. Expected: {parquet_path}")
        return None
    df = pd.read_parquet(parquet_path)
    if df.empty:
        st.warning("Data file is empty.")
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    
    df = df.sort_values('timestamp')
    df = df.dropna(subset=['timestamp'])

    if timeframe == "1m":
        return df

    df = df.set_index('timestamp')
    if timeframe.endswith('m'):
        rule = f"{int(timeframe[:-1])}min"
    elif timeframe.endswith('h'):
        rule = f"{int(timeframe[:-1])}H"
    else:
        st.error(f"Unsupported timeframe: {timeframe}")
        return None

    agg = df.resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()
    return agg


def compute_keltner_channel(df, ema_period=20, atr_period=20, atr_mult=2):
    # Middle line: EMA of Close
    kc_middle = df['close'].ewm(span=ema_period, adjust=False).mean()

    # True Range (Wilder’s method-compatible)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    # ATR using Wilder’s smoothing method
    atr = tr.ewm(alpha=1/atr_period, adjust=False).mean()

    # KC Bands
    kc_upper = kc_middle + atr_mult * atr
    kc_lower = kc_middle - atr_mult * atr

    return kc_middle, kc_upper, kc_lower


def plot_candles(df: pd.DataFrame):
    if df is None or df.empty:
        st.warning("No data to display yet.")
        return

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp_ist'] = df['timestamp'].dt.tz_convert('Asia/Kolkata')
    df['time_ist'] = df['timestamp_ist'].dt.time

    market_open = pd.to_datetime("09:15:00").time()
    market_close = pd.to_datetime("15:30:00").time()
    df = df[(df['time_ist'] >= market_open) & (df['time_ist'] <= market_close)]

    if df.empty:
        st.warning("No data in market hours (09:15–15:30 IST) to display.")
        return

    st.subheader(f"Candles for {symbol} ({timeframe}) [Market Hours Only]")

    ema, kc_upper, kc_lower = compute_keltner_channel(df)

    df['direction'] = df.apply(lambda row: "up" if row['close'] >= row['open'] else "down", axis=1)
    st.dataframe(df[['timestamp', 'open', 'high', 'low', 'close', 'direction']].tail(10), use_container_width=True)

    fig = go.Figure(data=[
        go.Candlestick(
            x=df['timestamp_ist'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            increasing_line_color='green',
            decreasing_line_color='red'
        ),
        go.Scatter(
            x=df['timestamp_ist'], y=ema,
            mode='lines', name='KC Middle',
            line=dict(color='blue', width=1, dash='dot')
        ),
        go.Scatter(
            x=df['timestamp_ist'], y=kc_upper,
            mode='lines', name='KC Upper',
            line=dict(color='orange', width=1)
        ),
        go.Scatter(
            x=df['timestamp_ist'], y=kc_lower,
            mode='lines', name='KC Lower',
            line=dict(color='orange', width=1)
        )
    ])



    fig.update_layout(
    xaxis=dict(
        type='date',
        rangeselector=dict(
            buttons=[
                dict(count=15, label="15m", step="minute", stepmode="backward"),
                dict(count=1, label="1h", step="hour", stepmode="backward"),
                dict(count=1, label="1d", step="day", stepmode="backward"),
                dict(step="all")
            ]
        ),
        rangeslider=dict(visible=True)
    ),
    yaxis=dict(
        autorange=True,   # ✅ Enable dynamic Y-axis
        fixedrange=False, # ✅ Allow zooming/panning on Y
    ),
    margin=dict(l=0, r=0, t=30, b=0),
    xaxis_title="Time",
    yaxis_title="Price",
    height=None,
    width=1000
)

    st.plotly_chart(fig, use_container_width=True, key=f"{symbol}_{timeframe}")
    st.dataframe(df.tail(20), use_container_width=True)


def get_file_mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


parquet_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", f"aggregator_{symbol}.parquet")
current_mtime = get_file_mtime(parquet_path)

if "last_mtime" not in st.session_state or st.session_state.get("last_symbol") != symbol or st.session_state.get("last_timeframe") != timeframe:
    st.session_state["last_mtime"] = None
    st.session_state["last_symbol"] = symbol
    st.session_state["last_timeframe"] = timeframe

refresh_needed = False
if current_mtime and st.session_state["last_mtime"] != current_mtime:
    refresh_needed = True
    st.session_state["last_mtime"] = current_mtime

if st.button("Refresh") or refresh_needed:
    candles = load_and_resample(symbol, timeframe)
    with plot_placeholder.container():
        plot_candles(candles)
else:
    st.info("No new data. Click 'Refresh' to check manually.")

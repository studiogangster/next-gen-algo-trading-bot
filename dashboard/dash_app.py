import os
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objs as go

# --- Config ---
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DEFAULT_SYMBOL = "2953217"
DEFAULT_TIMEFRAME = "1m"
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h"]

def get_available_symbols():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("aggregator_") and f.endswith(".parquet")]
    return [f.replace("aggregator_", "").replace(".parquet", "") for f in files]

def load_and_filter(symbol, timeframe):
    path = os.path.join(DATA_DIR, f"aggregator_{symbol}.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return df
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    # Market hours filter (IST)
    df['timestamp_ist'] = df['timestamp'].dt.tz_convert('Asia/Kolkata')
    df['time_ist'] = df['timestamp_ist'].dt.time
    market_open = pd.to_datetime("09:15:00").time()
    market_close = pd.to_datetime("15:30:00").time()
    df = df[(df['time_ist'] >= market_open) & (df['time_ist'] <= market_close)]
    if timeframe != "1m":
        df = df.set_index('timestamp')
        if timeframe.endswith('m'):
            rule = f"{int(timeframe[:-1])}min"
        elif timeframe.endswith('h'):
            rule = f"{int(timeframe[:-1])}H"
        else:
            return pd.DataFrame()
        df = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
    return df

def compute_keltner_channel(df, ema_period=20, atr_period=20, atr_mult=2):
    ema = df['close'].ewm(span=ema_period, adjust=False).mean()
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=atr_period, min_periods=1).mean()
    upper = ema + atr_mult * atr
    lower = ema - atr_mult * atr
    return ema, upper, lower

app = Dash(__name__)
app.title = "Stock Dashboard (Dash)"

app.layout = html.Div([
    html.H2("Stock Candlestick Dashboard (Dash)"),
    html.Div([
        html.Label("Symbol:"),
        dcc.Dropdown(
            id="symbol-dropdown",
            options=[{"label": s, "value": s} for s in get_available_symbols()],
            value=DEFAULT_SYMBOL,
            clearable=False,
            style={"width": "200px"}
        ),
        html.Label("Timeframe:"),
        dcc.Dropdown(
            id="timeframe-dropdown",
            options=[{"label": tf, "value": tf} for tf in TIMEFRAMES],
            value=DEFAULT_TIMEFRAME,
            clearable=False,
            style={"width": "120px"}
        ),
    ], style={"display": "flex", "gap": "2em", "alignItems": "center"}),
    dcc.Graph(id="candlestick-chart", style={"height": "600px"}),
    html.Div(id="debug-table")
])

@app.callback(
    Output("candlestick-chart", "figure"),
    Output("debug-table", "children"),
    Input("symbol-dropdown", "value"),
    Input("timeframe-dropdown", "value"),
)
def update_chart(symbol, timeframe):
    df = load_and_filter(symbol, timeframe)
    if df.empty:
        return go.Figure(), html.Div("No data available for this symbol/timeframe.")
    ema, kc_upper, kc_lower = compute_keltner_channel(df)
    # Candlestick trace
    candle = go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        increasing_line_color='lime',
        decreasing_line_color='red',
        name="Candles"
    )
    # KC overlays
    kc_middle = go.Scatter(
        x=df['timestamp'], y=ema, mode='lines', name='KC Middle', line=dict(color='deepskyblue', width=1, dash='dot')
    )
    kc_upper = go.Scatter(
        x=df['timestamp'], y=kc_upper, mode='lines', name='KC Upper', line=dict(color='orange', width=1)
    )
    kc_lower = go.Scatter(
        x=df['timestamp'], y=kc_lower, mode='lines', name='KC Lower', line=dict(color='orange', width=1)
    )
    # Volume bars
    volume = go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        marker_color=['lime' if c >= o else 'red' for c, o in zip(df['close'], df['open'])],
        name="Volume",
        yaxis="y2",
        opacity=0.4
    )
    # Compose figure with secondary y-axis for volume
    fig = go.Figure(data=[candle, kc_middle, kc_upper, kc_lower, volume])
    fig.update_layout(
        template="plotly_dark",
        xaxis_rangeslider_visible=True,
        xaxis_title="Time",
        yaxis=dict(title="Price", domain=[0.3, 1]),
        yaxis2=dict(title="Volume", domain=[0, 0.25], showgrid=False),
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(family="Roboto Mono, monospace", size=14),
        plot_bgcolor="#181a20",
        paper_bgcolor="#181a20",
        dragmode="pan",
        hovermode="x unified"
    )
    fig.update_traces(selector=dict(type="bar"), marker_line_width=0)
    # Debug table
    df_debug = df.copy()
    df_debug['direction'] = df_debug.apply(lambda row: "up" if row['close'] >= row['open'] else "down", axis=1)
    debug_table = html.Div([
        html.H4("Last 10 Candles"),
        dcc.Markdown(df_debug[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'direction']].tail(10).to_markdown())
    ])
    return fig, debug_table

if __name__ == "__main__":
    app.run(debug=True, port=8050)

import pandas as pd

from core.yaml_signal_engine import evaluate_rule_expr, generate_rule_signals, validate_strategy_params


def _sample_candles():
    return pd.DataFrame(
        {
            "timestamp": [1715000000, 1715000060, 1715000120, 1715000180, 1715000240],
            "open": [100, 102, 103, 101, 104],
            "high": [103, 104, 105, 106, 108],
            "low": [99, 101, 100, 99, 102],
            "close": [102, 103, 101, 105, 107],
            "volume": [1000, 1100, 1200, 1300, 1500],
        }
    ).set_index("timestamp")


def test_rule_expr_logical_and_comparators():
    context = {
        "rsi.value": 42,
        "close": 105,
        "sma.value": 101,
        "prev.close": 99,
        "prev.sma.value": 100,
    }
    expr = {
        "all": [
            {"gt": ["rsi.value", 30]},
            {"gt": ["close", "sma.value"]},
            {"cross_over": ["close", "sma.value"]},
        ]
    }
    assert evaluate_rule_expr(expr, context) is True


def test_generate_rule_signals_buy_and_sell():
    candles = _sample_candles()
    strategy_params = {
        "name": "rsi_sma_standard",
        "quantity": 1,
        "indicators": [
            {"id": "rsi", "type": "rsi", "params": {"length": 2}},
            {"id": "sma", "type": "sma", "params": {"length": 2}},
        ],
        "buy": {
            "all": [
                {"gt": ["rsi.value", 30]},
                {"gt": ["close", "sma.value"]},
            ]
        },
        "sell": {
            "all": [
                {"lt": ["rsi.value", 70]},
                {"lt": ["close", "sma.value"]},
            ]
        },
    }

    signals = generate_rule_signals(candles, strategy_params, symbol="256265", timeframe="1m")

    assert len(signals) > 0
    assert all(item["action"] in {"BUY", "SELL"} for item in signals)
    assert all(item["strategy"] == "rsi_sma_standard" for item in signals)
    assert all(item["timeframe"] == "1m" for item in signals)


def test_generate_rule_signals_with_cooldown_bars():
    candles = _sample_candles()
    base = {
        "name": "cooldown_test",
        "indicators": [
            {"id": "rsi", "type": "rsi", "params": {"length": 2}},
            {"id": "sma", "type": "sma", "params": {"length": 2}},
        ],
        "buy": {"gt": ["close", "sma.value"]},
        "sell": {"lt": ["close", "sma.value"]},
    }
    no_cooldown = generate_rule_signals(candles, {**base, "cooldown_bars": 0}, symbol="256265", timeframe="1m")
    with_cooldown = generate_rule_signals(candles, {**base, "cooldown_bars": 2}, symbol="256265", timeframe="1m")
    assert len(with_cooldown) <= len(no_cooldown)


def test_validate_strategy_params_reports_errors():
    errors = validate_strategy_params({"name": "", "quantity": -1, "cooldown_bars": -2})
    assert len(errors) >= 3

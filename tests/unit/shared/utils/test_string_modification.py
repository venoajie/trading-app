from src.shared.utils import string_modification as str_mod


def test_extract_currency_from_text():
    assert str_mod.extract_currency_from_text("chart.trades.BTC-PERPETUAL.1") == "btc"
    assert str_mod.extract_currency_from_text("incremental_ticker.ETH-4OCT24") == "eth"
    assert str_mod.extract_currency_from_text("BTC-PERPETUAL") == "btc"


def test_remove_redundant_elements():
    data = ["A", "A", "B", "C", "B"]
    assert str_mod.remove_redundant_elements(data) == ["A", "B", "C"]

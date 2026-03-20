import trading_core.gateway.binance_wsapi_userstream as us_mod
from trading_core.core.oms import FillEvent, OrderEvent
from trading_core.gateway.binance_wsapi_userstream import (
    BinanceWsApiConfig,
    BinanceWsApiUserStream,
)


def _make_stream() -> BinanceWsApiUserStream:
    return BinanceWsApiUserStream(
        cfg=BinanceWsApiConfig(
            api_key="k",
            api_secret="s",
            ws_api_url="wss://example.invalid/ws-api/v3",
        ),
        on_event=lambda _ev: None,
    )


def test_translate_execution_report_new_maps_to_ack_order_event():
    stream = _make_stream()
    out = stream._translate_execution_report(
        {
            "e": "executionReport",
            "c": "intent-1",
            "i": 123,
            "x": "NEW",
            "E": 1700000000000,
        }
    )

    assert out is not None
    assert len(out) == 1
    ev = out[0]
    assert isinstance(ev, OrderEvent)
    assert ev.type == "order"
    assert ev.intent_id == "intent-1"
    assert ev.exchange_order_id == "123"
    assert ev.status == "ACK"
    assert ev.ts_ns == 1_700_000_000_000_000_000


def test_translate_execution_report_rejected_includes_reason():
    stream = _make_stream()
    out = stream._translate_execution_report(
        {
            "e": "executionReport",
            "c": "intent-2",
            "i": 456,
            "x": "REJECTED",
            "r": "LOT_SIZE",
        }
    )

    assert out is not None
    assert len(out) == 1
    ev = out[0]
    assert isinstance(ev, OrderEvent)
    assert ev.status == "REJECTED"
    assert ev.reason == "LOT_SIZE"


def test_translate_execution_report_trade_emits_fill_and_partial_order():
    stream = _make_stream()
    out = stream._translate_execution_report(
        {
            "e": "executionReport",
            "c": "intent-3",
            "i": 789,
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "l": "0.25",
            "L": "101.5",
            "E": 1700000000001,
        }
    )

    assert out is not None
    assert len(out) == 2

    fill = out[0]
    order = out[1]

    assert isinstance(fill, FillEvent)
    assert fill.type == "fill"
    assert fill.intent_id == "intent-3"
    assert fill.exchange_order_id == "789"
    assert fill.qty == 0.25
    assert fill.price == 101.5
    assert fill.ts_ns == 1_700_000_000_001_000_000

    assert isinstance(order, OrderEvent)
    assert order.type == "order"
    assert order.status == "PARTIAL"


def test_translate_execution_report_uses_local_time_when_event_time_missing(
    monkeypatch,
):
    stream = _make_stream()
    monkeypatch.setattr(us_mod.time, "time_ns", lambda: 4242)

    out = stream._translate_execution_report(
        {"e": "executionReport", "c": "intent-4", "i": 999, "x": "CANCELED"}
    )

    assert out is not None
    assert len(out) == 1
    ev = out[0]
    assert isinstance(ev, OrderEvent)
    assert ev.ts_ns == 4242
    assert ev.status == "CANCELED"


def test_translate_execution_report_returns_none_for_invalid_trade_payload():
    stream = _make_stream()
    out = stream._translate_execution_report(
        {
            "e": "executionReport",
            "c": "intent-5",
            "i": 1000,
            "x": "TRADE",
            "l": "NaN?",
            "L": "100.0",
        }
    )

    assert out is None

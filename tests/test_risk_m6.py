from __future__ import annotations

import pytest

from trading_core.core.oms import OMS, OrderIntent, OrderEvent, FillEvent
from trading_core.core.risk import RiskConfig, RiskEngine

def _mk_risk(
        oms: OMS,
        *,
        max_position: float = 1.0,
        max_order_qty: float = 0.5,
        rate_capacity: float = 2.0,
        rate_per_sec: float = 0.0, # default 0 => deterministic rate-limit in tests
) -> RiskEngine:
    return RiskEngine(
        cfg=RiskConfig(
            max_position=max_position,
            max_order_qty=max_order_qty,
            rate_capacity=rate_capacity,
            rate_per_sec=rate_per_sec,
        ),
        oms=oms,
    )

def _intent(
        intent_id: str,
        *,
        symbol: str = "btcusdt",
        side: str = "buy",
        qty: float = 0.1,
        price: float = 100.0,
        ts_ns: int = 1_000,
) -> OrderIntent:
    return OrderIntent(
        type="intent",
        intent_id=intent_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=price,
        ts_ns=ts_ns,
    )

def _apply_fill(
        oms: OMS,
        *,
        intent_id: str,
        symbol: str,
        side: str,
        order_qty: float,
        fill_qty: float,
) -> None:
    """
    Build OMS state so RiskEngine can compute position from OMS fills.

    We:
    1) submit an intent to create OrderRecord with symbol/side/qty
    2) apply a fill event to increase filled_qty
    """
    oms.on_intent(
        _intent(intent_id, symbol=symbol, side=side, qty=order_qty, ts_ns=10)
    )
    oms.on_fill(
        FillEvent(
            type="fill",
            intent_id=intent_id,
            exchange_order_id="ex-1",
            qty=fill_qty,
            price=100.0,
            ts_ns=11
        )
    )

def test_rejects_bad_qty_and_side():
    oms = OMS()
    risk = _mk_risk(oms)

    # qty <= 0
    out = risk.check(_intent("i1", qty=0.0))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert out.reason.startswith("risk:")

    # bad side
    out = risk.check(_intent("i2", side="hold"))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert "bad_side" in out.reason

def test_rejects_max_order_qty_exceeded():
    oms = OMS()
    risk = _mk_risk(oms, max_order_qty=0.5)

    out = risk.check(_intent("i1", qty=0.51))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert "max_order_qty_exceeded" in out.reason

def test_rate_limiter_token_bucket_exhausts_then_rejects():
    """
    With rate_per_sec=0, no refill occurs.
    capacity=2 means 2 allowed, third must be rejected.
    """
    oms = OMS()
    risk = _mk_risk(oms, rate_capacity=2.0, rate_per_sec=0.0)

    # Same ts => no refill; determinstic
    assert risk.check(_intent("i1", ts_ns=100)) is None
    assert risk.check(_intent("i2", ts_ns=100)) is None

    out = risk.check(_intent("i3", ts_ns=100))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert "rate_limited" in out.reason

def test_rate_limiter_refills_over_time_allows_again():
    """
    capacity=1, refill_rate=1 token/sec.
    Consume one token at t=0, then at t=1s we can place again.
    """
    oms = OMS()
    risk = _mk_risk(oms, rate_capacity=1.0, rate_per_sec=1.0)

    assert risk.check(_intent("i1", ts_ns=int(1e9))) is None

    # Immediately again => rejected (no time passed)
    out = risk.check(_intent("i2", ts_ns=int(1e9)))
    assert isinstance(out, OrderEvent)
    assert "rate_limited" in out.reason

    # After 1 second => one token refilled => allowed
    out2 = risk.check(_intent("i3", ts_ns=int(2e9)))
    assert out2 is None

def test_rejects_max_position_exceeded_buy():
    """
    Current position +0.9 BTC, try buy +0.2 => projected 1.1 > 1.0 => reject.
    """
    oms = OMS()
    _apply_fill(
        oms,
        intent_id="pos-buy",
        symbol="btcusdt",
        side="buy",
        order_qty=1.0,
        fill_qty=0.9,
    )

    risk = _mk_risk(oms, max_position=1.0, max_order_qty=1.0, rate_capacity=100, rate_per_sec=0.0)

    out = risk.check(_intent("i1", symbol="btcusdt", side="buy", qty=0.2, ts_ns=100))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert "max_position_exceeded" in out.reason

def test_rejects_max_position_exceeded_sell_short():
    """
    Current position -0.8 BTC (from sells), try sell +0.3 => projected -1.1 < -1.0 => reject.
    """
    oms = OMS()
    _apply_fill(
        oms,
        intent_id="pos-sell",
        symbol="btcusdt",
        side="sell",
        order_qty=1.0,
        fill_qty=0.8,
    )

    risk = _mk_risk(oms, max_position=1.0, max_order_qty=1.0, rate_capacity=100, rate_per_sec=0.0)

    out = risk.check(_intent("i1", symbol="btcusdt", side="sell", qty=0.3, ts_ns=100))
    assert isinstance(out, OrderEvent)
    assert out.status == "REJECTED"
    assert "max_position_exceeded" in out.reason

def test_allows_valid_intent():
    oms = OMS()
    risk = _mk_risk(oms, max_position=1.0, max_order_qty=0.5, rate_capacity=10, rate_per_sec=0.0)

    out = risk.check(_intent("ok", qty=0.1, ts_ns=100))
    assert out is None
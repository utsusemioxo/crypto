from trading_core.core.oms import OMS, OrderIntent, OrderEvent, FillEvent, OrderStatus


def test_duplicate_ack_does_not_break():
    oms = OMS()

    it = OrderIntent(
        type="intent",
        intent_id="i1",
        symbol="btcusdt",
        side="buy",
        qty=1.0,
        price=100.0,
        ts_ns=1,
    )
    oms.on_intent(it)

    ack = OrderEvent(
        type="order", intent_id="i1", exchange_order_id="ex1", status="ACK", ts_ns=2
    )
    oms.on_order_event(ack)
    oms.on_order_event(ack)  # duplicate

    rec = oms.by_intent["i1"]
    assert rec.exchange_order_id == "ex1"
    assert rec.status == OrderStatus.ACK


def test_duplicate_fill_does_not_double_count():
    oms = OMS()
    oms.on_intent(
        OrderIntent(
            type="intent",
            intent_id="i1",
            symbol="btcusdt",
            side="buy",
            qty=1.0,
            price=100,
            ts_ns=1,
        )
    )
    oms.on_order_event(
        OrderEvent(
            type="order", intent_id="i1", exchange_order_id="ex1", status="ACK", ts_ns=2
        )
    )

    f1 = FillEvent(
        type="fill",
        intent_id="i1",
        exchange_order_id="ex1",
        qty=0.4,
        price=101.0,
        ts_ns=10,
    )
    oms.on_fill(f1)
    oms.on_fill(f1)  # duplicate

    rec = oms.by_intent["i1"]
    assert rec.filled_qty == 0.4
    assert rec.status == OrderStatus.PARTIAL


def test_out_of_order_fill_before_ack():
    oms = OMS()

    # Intent exists but no ACK yet
    oms.on_intent(
        OrderIntent(
            type="intent",
            intent_id="i1",
            symbol="btcusdt",
            side="buy",
            qty=1.0,
            price=100,
            ts_ns=1,
        )
    )

    # Fill arrives first (out-of-order)
    oms.on_fill(
        FillEvent(
            type="fill",
            intent_id="i1",
            exchange_order_id="ex1",
            qty=1.0,
            price=99.5,
            ts_ns=5,
        )
    )

    # Then ACK arrives
    oms.on_order_event(
        OrderEvent(
            type="order", intent_id="i1", exchange_order_id="ex1", status="ACK", ts_ns=6
        )
    )

    rec = oms.by_intent["i1"]
    # Must end in FILLED, and ACK must not downgrade
    assert rec.exchange_order_id == "ex1"
    assert rec.status == OrderStatus.FILLED
    assert rec.filled_qty == 1.0

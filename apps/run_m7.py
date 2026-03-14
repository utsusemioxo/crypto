from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

load_dotenv()

from trading_core.bus.bus import EventBus
from trading_core.core import monotonic_ns
from trading_core.core.metrics_m7 import LatencyTracker, MetricsReporter
from trading_core.core.oms import OMS, OrderEvent, OrderIntent
from trading_core.core.risk import RiskConfig, RiskEngine
from trading_core.gateway.binance_rest_exec import (
    BinanceRestConfig,
    BinanceRestExecutionGateway,
)
from trading_core.gateway.binance_wsapi_userstream import (
    BinanceWsApiConfig,
    BinanceWsApiUserStream,
)


async def main() -> None:
    """
    Minimal M5 + M6 + M7 wiring:
    - EventBus + OMS
    - Risk engine
    - REST execution
    - WS API v3 user stream
    - Latency tracking and reporting
    """
    bus = EventBus(max_queue=200_000)
    oms = OMS()

    # ---- OMS consumes lifecycle and fill events ----
    bus.subscribe("intent", oms.on_intent)
    bus.subscribe("order", oms.on_order_event)
    bus.subscribe("fill", oms.on_fill)

    # ---- M6: Risk engine ----
    risk = RiskEngine(
        cfg=RiskConfig(
            max_position=0.001,   # absolute net position limit
            max_order_qty=0.0003, # per-order qty limit
            rate_capacity=5,      # burst
            rate_per_sec=2,       # steady rate
        ),
        oms=oms,
    )

    # ---- M7: Latency tracking ----
    tracker = LatencyTracker(spike_threshold_ms=200.0)
    bus.subscribe("intent", tracker.on_intent)
    bus.subscribe("order", tracker.on_order_event)
    bus.subscribe("fill", tracker.on_fill)

    # ---- Binance configs (testnet first) ----
    api_key = os.environ["BINANCE_TESTNET_API_KEY"]
    api_secret = os.environ["BINANCE_TESTNET_API_SECRET"]

    rest = BinanceRestExecutionGateway(
        BinanceRestConfig(
            api_key=api_key,
            api_secret=api_secret,
            rest_base=os.environ.get(
                "BINANCE_REST_BASE",
                "https://testnet.binance.vision",
            ),
        )
    )

    user_stream = BinanceWsApiUserStream(
        BinanceWsApiConfig(
            api_key=api_key,
            api_secret=api_secret,
            ws_api_url=os.environ.get(
                "BINANCE_WSAPI_URL",
                "wss://ws-api.testnet.binance.vision/ws-api/v3",
            ),
        ),
        on_event=bus.publish,
    )

    # ---- Start user stream first (source of truth for order lifecycle) ----
    t_us = asyncio.create_task(user_stream.run_forever())
    t_metrics = asyncio.create_task(
        MetricsReporter(tracker, interval_sec=5.0).run_forever()
    )

    # ---- Consume intents: risk gate -> place via REST ----
    async def handle_intent(it: OrderIntent) -> None:
        # M6: risk check must happen BEFORE execution
        rej = risk.check(it)
        if rej is not None:
            bus.publish(rej)  # recorder can persist this as an order event
            return

        # M7: local send timing
        tracker.mark_send_start(it.intent_id, monotonic_ns())
        try:
            await rest.place_limit(it)
            # Do NOT emit ACK from REST response.
            # User stream executionReport is the source of truth.
        except Exception as e:
            bus.publish(
                OrderEvent(
                    type="order",
                    intent_id=it.intent_id,
                    exchange_order_id="",
                    status="REJECTED",
                    ts_ns=it.ts_ns,
                    reason=f"rest_error:{type(e).__name__}:{e}",
                )
            )
        finally:
            tracker.mark_send_done(it.intent_id, monotonic_ns())

    bus.subscribe("intent", lambda it: asyncio.create_task(handle_intent(it)))

    # ---- Manual smoke test intent ----
    bus.publish(
        OrderIntent(
            type="intent",
            intent_id="m7-i1",
            symbol="btcusdt",
            side="buy",
            qty=0.0001,
            price=100.0,  # far away: likely ACK but no fill
            ts_ns=monotonic_ns(),
        )
    )

    try:
        while True:
            bus.run_batch(50_000)
            await asyncio.sleep(0.01)
    finally:
        t_us.cancel()
        t_metrics.cancel()
        await asyncio.gather(t_us, t_metrics, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
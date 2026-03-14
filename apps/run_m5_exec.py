from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
load_dotenv()

from trading_core.bus.bus import EventBus
from trading_core.core.oms import OMS, OrderEvent, OrderIntent
from trading_core.gateway.binance_rest_exec import BinanceRestConfig, BinanceRestExecutionGateway
from trading_core.gateway.binance_wsapi_userstream import BinanceWsApiConfig, BinanceWsApiUserStream
from trading_core.core import monotonic_ns, risk

async def main() -> None:
    """
    Minimal M5 wiring:
    - EventBus + OMS (M3/M4)
    - REST execution (place orders)
    - WS API v3 user stream (receive execution reports)
    """
    bus = EventBus(max_queue=200_000)
    oms = OMS()

    # OMS consumes lifecycle and fill events
    bus.subscribe("intent", oms.on_intent)
    bus.subscribe("order", oms.on_order_event)
    bus.subscribe("fill", oms.on_fill)

    # ---- Binance configs (use testnet first) ----
    api_key = os.environ["BINANCE_TESTNET_API_KEY"]
    api_secret = os.environ["BINANCE_TESTNET_API_SECRET"]

    rest = BinanceRestExecutionGateway(
        BinanceRestConfig(
            api_key=api_key,
            api_secret=api_secret,
            rest_base=os.environ.get("BINANCE_REST_BASE", "https://testnet.binance.vision"),
        )
    )

    user_stream = BinanceWsApiUserStream(
        BinanceWsApiConfig(
            api_key=api_key,
            api_secret=api_secret,
            ws_api_url=os.environ.get("BINANCE_WSAPI_URL", "wss://ws-api.testnet.binance.vision/ws-api/v3"),
        ),
        on_event=bus.publish,
    )

    # Start user stream first (source of truth for order lifecycle)
    t_us = asyncio.create_task(user_stream.run_forever())

    # Consume intents: place via REST (async handler)
    async def handle_intent(it: OrderIntent) -> None:
        try:
            _ = await rest.place_limit(it)
            # We do NOT emit ACK based REST reponse here.
            # The user stream executionReport is treated as truth.
        except Exception as e:
            # Emit a REJECT-like event when we know the REST call failed locally.
            # Note: a timeout is ambiguous in reality; for M5 minimal, treat it as error but do not retry.
            rej = risk.check(it)
            if rej is not None:
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
    
    bus.subscribe("intent", lambda it: asyncio.create_task(handle_intent(it)))

    # ---- Manual smoke test intent ----
    bus.publish(
        OrderIntent(
            type="intent",
            intent_id="m5-i1",
            symbol="btcusdt",
            side="buy",
            qty=0.0001,
            price=100.0, # far away: should ACK but not fill quickly
            ts_ns=monotonic_ns(),
        )
    )

    try:
        while True:
            bus.run_batch(50_000)
            await asyncio.sleep(0.01)
    finally:
        t_us.cancel()

if __name__ == "__main__":
    asyncio.run(main())
from __future__ import annotations

import asyncio
import contextlib
import os

from dotenv import load_dotenv

from trading_core.bus.bus import EventBus
from trading_core.core import monotonic_ns
from trading_core.core.oms import OMS, OrderEvent, OrderIntent
from trading_core.gateway.ibkr_ibasync_gateway import IbkrIbAsyncConfig, IbkrIbAsyncGateway

load_dotenv()


async def main() -> None:
    bus = EventBus(max_queue=200_000)
    oms = OMS()

    bus.subscribe("intent", oms.on_intent)
    bus.subscribe("order", oms.on_order_event)
    bus.subscribe("fill", oms.on_fill)

    gateway = IbkrIbAsyncGateway(
        cfg=IbkrIbAsyncConfig(
            host=os.environ.get("IBKR_HOST", "127.0.0.1"),
            port=int(os.environ.get("IBKR_PORT", "7497")),
            client_id=int(os.environ.get("IBKR_CLIENT_ID", "19")),
            account=os.environ.get("IBKR_ACCOUNT", ""),
            default_exchange=os.environ.get("IBKR_EXCHANGE", "SMART"),
            default_currency=os.environ.get("IBKR_CURRENCY", "USD"),
        ),
        on_event=bus.publish,
    )

    t_gw = asyncio.create_task(gateway.run_forever())

    async def handle_intent(it: OrderIntent) -> None:
        try:
            await gateway.place_limit(it)
            # Do not synthesize ACK from placement response; IBKR status stream is source of truth.
        except Exception as e:
            bus.publish(
                OrderEvent(
                    type="order",
                    intent_id=it.intent_id,
                    exchange_order_id="",
                    status="REJECTED",
                    ts_ns=it.ts_ns,
                    reason=f"ibkr_place_error:{type(e).__name__}:{e}",
                )
            )

    bus.subscribe("intent", lambda it: asyncio.create_task(handle_intent(it)))

    # Manual smoke test: far-away buy limit so it usually ACKs but does not fill immediately.
    bus.publish(
        OrderIntent(
            type="intent",
            intent_id="ibkr-i1",
            symbol=os.environ.get("IBKR_SMOKE_SYMBOL", "AAPL"),
            side="buy",
            qty=float(os.environ.get("IBKR_SMOKE_QTY", "1")),
            price=float(os.environ.get("IBKR_SMOKE_LIMIT", "1")),
            ts_ns=monotonic_ns(),
        )
    )

    try:
        while True:
            bus.run_batch(50_000)
            await asyncio.sleep(0.01)
    finally:
        t_gw.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t_gw


if __name__ == "__main__":
    asyncio.run(main())

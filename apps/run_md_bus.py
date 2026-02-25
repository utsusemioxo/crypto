import asyncio
from pathlib import Path

from trading_core.bus.bus import EventBus
from trading_core.gateway.binance_md import BinanceMarketDataGateway
from trading_core.recorder.recorder import NdjsonRecorder

async def main() -> None:
    bus = EventBus(max_queue=200_000)

    rec = NdjsonRecorder(path=Path("data") / "md-live-btcusdt.ndjson")
    rec.open()
    print(f"[rec] session_id={rec.session_id} path={rec.path}")

    # Handlers (subscribers)
    bus.subscribe("market", rec.append)

    # Gateway publishes into bus (instead of calling recorder directly)
    gw = BinanceMarketDataGateway(
        symbol="btcusdt",
        on_event=bus.publish,
        source="binance"
    )

    try:
        # Run gateway in background task
        gw_task = asyncio.create_task(gw.run_forever())

        # Single-thread event loop: drain the queue
        while True:
            processed = bus.run_batch(5000)
            if processed == 0:
                await asyncio.sleep(0) # yield to other tasks
    
    finally:
        rec.close()
        gw_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
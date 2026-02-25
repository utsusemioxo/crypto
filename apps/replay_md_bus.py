import asyncio
from pathlib import Path

from trading_core.bus.bus import EventBus
from trading_core.gateway.replay_md import ReplayMarketDataGateway
from trading_core.recorder.recorder import NdjsonRecorder

async def main() -> None:
    bus = EventBus(max_queue=200_000)

    out = NdjsonRecorder(path=Path("data") / "md-replay-out.ndjson")
    out.open()
    print(f"[rec] replay session_id={out.session_id} path={out.path}")

    bus.subscribe("market", out.append)

    rgw = ReplayMarketDataGateway(
        path=Path("data") / "md-live-btcusdt.ndjson",
        on_event=bus.publish,
        speed=0.0,
        only_symbol="btcusdt",
    )

    # replay is finite: run it then drain remaining events
    await rgw.run()
    while bus.run_batch(10000):
        pass

    out.close()

if __name__ == "__main__":
    asyncio.run(main())
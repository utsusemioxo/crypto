import asyncio
from pathlib import Path

from trading_core.gateway.replay_md import ReplayMarketDataGateway
from trading_core.recorder.recorder import NdjsonRecorder


async def main() -> None:
    out = NdjsonRecorder(path=Path("data") / "md-replay-out.ndjson")
    out.open()
    print(f"[rec] replay session_id={out.session_id} path={out.path}")

    rgw = ReplayMarketDataGateway(
        path=Path("data") / "md-live-btcusdt.ndjson",
        on_event=out.append,  # replay -> SAME interface as live
        speed=20.0,
        only_symbol="btcusdt",
    )

    try:
        await rgw.run()
    finally:
        out.close()


if __name__ == "__main__":
    asyncio.run(main())

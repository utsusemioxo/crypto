from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from trading_core.gateway.binance_md import BinanceMarketDataGateway
from trading_core.recorder.recorder import NdjsonRecorder


async def main() -> None:
    rec = NdjsonRecorder(path=Path("data") / "md-live-btcusdt.ndjson")
    rec.open()
    print(f"[rec] session_id={rec.session_id} path={rec.path}")

    gw = BinanceMarketDataGateway(
        symbol="btcusdt", on_event=rec.append, source="binance"
    )

    try:
        await gw.run_forever()
    finally:
        rec.close()


if __name__ == "__main__":
    asyncio.run(main())

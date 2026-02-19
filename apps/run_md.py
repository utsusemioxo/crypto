from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from trading_core.gateway.binance_md import BinanceMarketDataGateway
from trading_core.recorder.recorder import NdjsonRecorder

async def main() -> None:
    symbol = "btcusdt"
    session = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out = Path("data") / f"md-{symbol}-{session}.ndjson"

    rec = NdjsonRecorder(out)

    gw = BinanceMarketDataGateway(
        symbol=symbol,
        on_event=rec.append,
    )
    await gw.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable
import asyncio
import json

from trading_core.core import MarketEvent

@dataclass(slots=True)
class ReplayMarketDataGateway:
    """
    Replay NDJSON -> MarketEvent stream.

    speed:
      - 1.0 = real-time pacing by ts_recv_ns deltas
      - >1  = faster (e.g., 20.0 = 20x)
      - 0,0 = no sleep (as fast as possible)

    "Same Behavior" means: replays emits the same internal Market schema
    through the same on_event callback used by live.
    """
    path: Path
    on_event: Callable[[MarketEvent], None]
    speed: float = 1.0
    only_symbol: str | None = None # e.g. "btcusdt"

    async def run(self) -> None:
        prev_ts: int | None = None

        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # tolerate partial/corrupted last line: skip if not valid JSON
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if d.get("type") != "market":
                    continue

                sym = str(d.get("symbol", "")).lower()
                if not sym:
                    continue
                if self.only_symbol and sym != self.only_symbol.lower():
                    continue

                ts = int(d.get("ts_recv_ns", 0))

                # pacing
                if prev_ts is not None and self.speed > 0:
                    dt_ns = ts - prev_ts
                    if dt_ns > 0:
                        await asyncio.sleep((dt_ns / 1e9) / self.speed)
                prev_ts = ts

                ev = MarketEvent.new(
                    symbol=sym,
                    seq=int(d.get("seq", 0)),
                    ts_recv_ns=ts,
                    ts_exchange_ns=int(d.get("ts_exchange_ns", 0)),
                    bid=float(d.get("bid", 0.0)),
                    ask=float(d.get("ask", 0.0)),
                    last=float(d.get("last", 0.0)),
                    bid_sz=float(d.get("bid_sz", 0.0)),
                    ask_sz=float(d.get("ask_sz", 0.0)),
                    last_sz=float(d.get("last_sz", 0.0)),
                    source=str(d.get("source", "replay")),
                )

                self.on_event(ev)

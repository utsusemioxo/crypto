from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import asyncio
import json
import random

import websockets
from websockets import WebSocketClientProtocol

from trading_core.core import MarketEvent, monotonic_ns

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"


@dataclass(slots=True)
class SeqTracker:
    """
    Local sequence tracker.

    We generate seq locally (monotonic increasing) and detect:
    - gaps (should not happen locally)
    - discontinuity across reconnect (expected, but we mark it)
    """

    seq: int = 0
    last_session: int = 0

    def next(self) -> int:
        self.seq += 1
        return self.seq

    def new_session(self) -> None:
        self.last_session += 1


@dataclass(slots=True)
class BinanceMarketDataGateway:
    """
    Minimal Binance WS MarketData Gateway (bookTicker)

    Responsibilities:
    - connect to WS
    - parse messages -> MarketEvent
    - local seq generation
    - expose callback to consume MarketEvent (e.g., recorder.append)
    """

    symbol: str
    on_event: Callable[[MarketEvent], None]
    source: str = "binance"
    _tracker: SeqTracker = field(default_factory=SeqTracker)

    def _ws_url(self) -> str:
        # bookTicker stream: <symbol>@bookTicker
        stream = f"{self.symbol.lower()}@bookTicker"
        return f"{BINANCE_WS_BASE}/{stream}"

    async def run_forever(self) -> None:
        backoff_s = 0.5
        max_backoff_s = 20.0

        while True:
            self._tracker.new_session()
            url = self._ws_url()

            try:
                print(f"[md] connect session={self._tracker.last_session} url={url}")
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=1000,  # prevent unbounded memory growth
                ) as ws:
                    print(f"[md] connected session={self._tracker.last_session}")
                    await self._consume(ws)
                # Normal close -> reset backoff
                backoff_s = 0.5

            except asyncio.CancelledError:
                raise

            except Exception as e:
                print(
                    f"[md] disconnect session={self._tracker.last_session} err={type(e).__name__}: {e}"
                )
                # backoff with jitter
                jitter = random.uniform(0, 0.3)
                sleep_s = min(max_backoff_s, backoff_s) + jitter
                # You can log this; keep minimal for now
                print(f"[md] error={e!r}, reconnect in {sleep_s:.2f}s")
                await asyncio.sleep(sleep_s)
                backoff_s *= 1.8

    async def _consume(self, ws: WebSocketClientProtocol) -> None:
        last_msg_ns = monotonic_ns()

        recv_timeout_s = 3
        stale_timeout_ns = int(15 * 1e9)  # 15s 没消息认为连接挂了(可以调节)

        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout_s)
            except asyncio.TimeoutError:
                now = monotonic_ns()
                if now - last_msg_ns > stale_timeout_ns:
                    raise RuntimeError("stale websocket: no messages")
                continue

            ts_recv = monotonic_ns()
            last_msg_ns = ts_recv

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            me = self._parse_book_ticker(raw, ts_recv_ns=ts_recv)
            if me is not None:
                self.on_event(me)

    def _parse_book_ticker(self, raw: str, *, ts_recv_ns: int) -> MarketEvent | None:
        """
        Binance bookTicker payload (typical)
        {
            "u":400900217,
            "s":"BNBUSDT",
            "b":"25.35190000",
            "B":"31.21000000",
            "a":"25.36520000",
            "A":"40.66000000"
        }
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return None

        # Defensive: ignore messages not matching expected format
        symbol = msg.get("s")
        if not symbol:
            return None

        bid = float(msg.get("b", 0.0))
        bid_sz = float(msg.get("B", 0.0))
        ask = float(msg.get("a", 0.0))
        ask_sz = float(msg.get("A", 0.0))

        seq = self._tracker.next()

        # bookTicker doesn't include exchange ts in this payload; set 0 for now.
        return MarketEvent.new(
            symbol=symbol.lower(),
            seq=seq,
            ts_recv_ns=ts_recv_ns,
            ts_exchange_ns=0,
            bid=bid,
            ask=ask,
            bid_sz=bid_sz,
            ask_sz=ask_sz,
            source=self.source,
        )

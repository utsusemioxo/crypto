from __future__ import annotations

import asyncio
import hmac
import hashlib
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import websockets
from websockets import WebSocketClientProtocol
from trading_core.core.oms import OrderEvent, FillEvent


def _hmac_sha256_hex(secret: str, payload: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def _sorted_query_string(params: Dict[str, Any]) -> str:
    """
    Build a stable query-string payload for HMAC signing.

    Notes:
    - Binance signs a query-string style payload.
    - Keep ordering stable by sorting keys.
    - Values here are simple ASCII (ints/strings), so no special encoding is needed for this minimal M5.
    """
    items = sorted(params.items(), key=lambda kv: kv[0])
    return "&".join(f"{k}={v}" for k, v in items)


@dataclass(slots=True)
class BinanceWsApiConfig:
    """
    WebSocket API v3 configuration for user data stream.

    Endpoints:
    - Prod:    wss://ws-api.binance.com:443/ws-api/v3
    - Testnet: wss://ws-api.testnet.binance.vision/ws-api/v3
    """

    api_key: str
    api_secret: str
    ws_api_url: str

    recv_window_ms: int = 5000
    ping_interval_s: int = 20
    ping_timeout_s: int = 20
    close_timeout_s: int = 5
    max_queue: int = 2000


@dataclass(slots=True)
class BinanceWsApiUserStream:
    """
    User data stream via Binance WebSocket API v3.

    Responsibilities:
    - Maintain a private WS connection
    - Subscribe using userDataStream.subscribe.signature (HMAC)
    - Convert executionReport into internal OrderEvent / FillEvent

    Boundaries:
    - Does NOT keep order state (OMS is the source of truth)
    - Must tolerate duplicates and out-of-order messages (OMS handles idempotency/state)
    """

    cfg: BinanceWsApiConfig
    on_event: Callable[[object], None]  # publish callback into your EventBus

    async def run_forever(self) -> None:
        """
        Run forever with reconnect + expoential backoff.
        """
        backoff_s = 0.5
        max_backoff_s = 20.0

        while True:
            try:
                print(f"[us] connect url={self.cfg.ws_api_url}")
                async with websockets.connect(
                    self.cfg.ws_api_url,
                    ping_interval=self.cfg.ping_interval_s,
                    ping_timeout=self.cfg.ping_timeout_s,
                    close_timeout=self.cfg.close_timeout_s,
                    max_queue=self.cfg.max_queue,
                ) as ws:
                    print("[us] connected")
                    sub_id = await self._subscribe_signature(ws)
                    print(f"[us] subscribed subscriptionId={sub_id}")
                    backoff_s = 0.5
                    await self._consume(ws)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[us] disconnect err={type(e).__name__}: {e}")
                jitter = random.uniform(0, 0.3)
                await asyncio.sleep(min(max_backoff_s, backoff_s) + jitter)
                backoff_s *= 1.8

    async def _subscribe_signature(self, ws: WebSocketClientProtocol) -> int:
        """
        Subscribe to user data stream using a signed request.

        This avoids the old listenKey lifecycle (POST/PUT keepalive), and works with WS API v3.
        """
        params = {
            "apiKey": self.cfg.api_key,
            "timestamp": int(time.time() * 1000),
            "recvWindow": self.cfg.recv_window_ms,
        }
        payload = _sorted_query_string(params)
        params["signature"] = _hmac_sha256_hex(self.cfg.api_secret, payload)

        req = {
            "id": f"sub-{int(time.time() * 1000)}",
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }
        await ws.send(json.dumps(req))

        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("status") != 200:
            raise RuntimeError(f"subscribe failed: {msg}")

        result = msg.get("result") or {}
        return int(result.get("subscruptionId", 0))

    async def _consume(self, ws: WebSocketClientProtocol) -> None:
        """
        Consume subscription frames:
          {"subscriptionId": 0, "event": {...}}
        """
        async for raw in ws:
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                continue

            ev = frame.get("event")
            if not isinstance(ev, dict):
                continue

            if ev.get("e") == "executionReport":
                out = self._translate_execution_report(ev)
                if out is not None:
                    for e in out:
                        self.on_event(e)

    def _translate_execution_report(self, ev: Dict[str, Any]) -> Optional[list[object]]:
        """
        Translate Binance executionReport -> OrderEvent / FillEvent.

        We treat:
        - clientOrderId ("c") as intent_id (because we set newClientOrderId=intent_id)
        - orderId ("i") as exchange_order_id

        Execution type ("x"):
        - NEW, REJECTED, CANCELED, REJECTED, EXPIRED ...
        """
        # Required identifiers
        intent_id = ev.get("c")
        exch_id = ev.get("i")
        x = ev.get("x")  # execution type
        X = ev.get("X")  # order status

        if not intent_id or exch_id is None or not x:
            return None

        intent_id = str(intent_id)
        exchange_order_id = str(exch_id)

        # Prefer exchange-provided event time if present; fall back to local time.
        # E is event time (ms) for spot executionReport in many payloads.
        E_ms = ev.get("E")
        ts_ns = int(E_ms) * 1_000_000 if isinstance(E_ms, int) else time.time_ns()

        out: list[object] = []

        # Order lifecycle signals
        if x in ("NEW", "REJECTED", "CANCELED", "EXPIRED"):
            status_map = {
                "NEW": "ACK",
                "REJECTED": "REJECTED",
                "CANCELED": "CANCELED",
                "EXPIRED": "CANCELED",  # Minimal M5: treat EXPIRED as canceled-like terminal
            }
            reason = str(ev.get("r", "")) if x == "REJECTED" else ""
            out.append(
                OrderEvent(
                    type="order",
                    intent_id=intent_id,
                    exchange_order_id=exchange_order_id,
                    status=status_map[x],
                    ts_ns=ts_ns,
                    reason=reason,
                )
            )
            return out

        # Trade fills
        if x == "TRADE":
            try:
                last_qty = float(ev.get("l", 0.0))  # last executed quantity
                last_px = float(ev.get("L", 0.0))  # last executed price
            except TypeError, ValueError:
                return None

            if last_qty > 0:
                out.append(
                    FillEvent(
                        type="fill",
                        intent_id=intent_id,
                        exchange_order_id=exchange_order_id,
                        qty=last_qty,
                        price=last_px,
                        ts_ns=ts_ns,
                    )
                )

            # Optional: also emit a coarse order status hint from X.
            # OMS can infer from fills, but explicit terminal/partial signals are useful in practice.
            if X in ("PARTIALLY_FILLED", "FILLED"):
                out.append(
                    OrderEvent(
                        type="order",
                        intent_id=intent_id,
                        exchange_order_id=exchange_order_id,
                        status="PARTIAL" if X == "PARTIALLY_FILLED" else "FILLED",
                        ts_ns=ts_ns,
                        reason="",
                    )
                )
            return out

        return None

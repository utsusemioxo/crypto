from __future__ import annotations

import hmac
import hashlib
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from trading_core.core.oms import OrderIntent


def _hmac_sha256_hex(secret: str, payload: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def _signed_query(params: Dict[str, Any], api_secret: str) -> str:
    """
    Build a signed query string for Binance REST.

    Notes:
    - Sort keys for stable signing.
    - Use percent-encoding via urlencode.
    """
    items = sorted(params.items(), key=lambda kv: kv[0])
    qs = urllib.parse.urlencode(items, doseq=True, safe="")
    sig = _hmac_sha256_hex(api_secret, qs)
    return f"{qs}&signature={sig}"


@dataclass(slots=True)
class BinanceRestConfig:
    """
    REST config.

    Typical endpoints:
    - Spot prod:    https://api.binance.com
    - Spot testnet: https://testnet.binance.vision
    """

    api_key: str
    api_secret: str
    rest_base: str

    recv_window_ms: int = 5000
    timeout_s: float = 10.0


@dataclass(slots=True)
class BinanceRestExecutionGateway:
    """
    REST order placement gateway.

    Responsibilities:
    - Place orders with newClientOrderId=intent_id for idempotency
    - Do NOT treat REST response as the source of truth (user stream is the truth)
    """

    cfg: BinanceRestConfig

    async def place_limit(
        self, it: OrderIntent, *, time_in_force: str = "GTC"
    ) -> Dict[str, Any]:
        """
        Place a LIMIT order.

        Idempotency:
        - newClientOrderId = intent_id
        - If we retry (we generally should not auto-retry on timeout), exchange can de-duplicate.
        """
        url = f"{self.cfg.rest_base}/api/v3/order"
        params: Dict[str, Any] = {
            "symbol": it.symbol.upper(),
            "side": it.side.upper(),
            "type": "LIMIT",
            "timeInForce": time_in_force,
            "quantity": f"{it.qty:.8f}",
            "price": f"{it.price:.8f}",
            "newClientOrderId": it.intent_id,
            "recvWindow": self.cfg.recv_window_ms,
            "timestamp": int(time.time() * 1000),
        }
        body = _signed_query(params, self.cfg.api_secret)

        headers = {"X-MBX-APIKEY": self.cfg.api_key}

        async with httpx.AsyncClient(timeout=self.cfg.timeout_s) as client:
            resp = await client.post(url, content=body, headers=headers)
            resp.raise_for_status()
            return resp.json()

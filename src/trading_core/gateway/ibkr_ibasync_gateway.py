from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from trading_core.core.oms import FillEvent, OrderEvent, OrderIntent


def _status_to_order_event(status: str) -> Optional[str]:
    """
    Map IBKR order status -> internal OMS status.

    Known IBKR statuses include:
    PendingSubmit, ApiPending, PreSubmitted, Submitted,
    PendingCancel, ApiCancelled, Cancelled, Filled, Inactive.
    """
    s = (status or "").strip()
    if s in ("PendingSubmit", "ApiPending", "PreSubmitted", "Submitted"):
        return "ACK"
    if s in ("PendingCancel", "ApiCancelled", "Cancelled"):
        return "CANCELED"
    if s == "Filled":
        return "FILLED"
    if s == "Inactive":
        return "REJECTED"
    return None


def _to_ns_from_time_like(v: Any) -> int:
    """
    Convert IBKR time-like values into nanoseconds.

    Supports:
    - unix seconds as int/float
    - datetime-like values with .timestamp()
    - fallback to local time_ns when absent/unknown
    """
    if isinstance(v, (int, float)):
        return int(float(v) * 1_000_000_000)

    ts_fn = getattr(v, "timestamp", None)
    if callable(ts_fn):
        try:
            return int(float(ts_fn()) * 1_000_000_000)
        except Exception:
            pass

    return time.time_ns()


@dataclass(slots=True)
class IbkrIbAsyncConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 19
    connect_timeout_s: float = 10.0

    account: str = ""
    default_exchange: str = "SMART"
    default_currency: str = "USD"


@dataclass(slots=True)
class IbkrIbAsyncGateway:
    """
    IBKR execution + user event gateway via ib_async.

    Responsibilities:
    - Connect/reconnect to IBKR TWS or IB Gateway.
    - Place limit orders with `orderRef=intent_id`.
    - Translate order status and execution/fill updates into OrderEvent / FillEvent.

    Notes:
    - This class intentionally keeps translation logic lightweight and lets OMS
      remain the source of truth for idempotency/state transitions.
    - `ib_async` API is expected to be ib_insync-compatible for IB/Trade events.
    """

    cfg: IbkrIbAsyncConfig
    on_event: Callable[[object], None]

    _ib: Any = field(default=None, init=False, repr=False)
    _orderid_to_intent: Dict[int, str] = field(default_factory=dict, init=False, repr=False)

    async def connect(self) -> None:
        ib_mod = self._import_ib_async()
        self._ib = ib_mod.IB()

        await self._ib.connectAsync(
            self.cfg.host,
            self.cfg.port,
            clientId=self.cfg.client_id,
            timeout=self.cfg.connect_timeout_s,
        )
        self._attach_handlers()
        print(f"[ibkr] connected host={self.cfg.host} port={self.cfg.port} client_id={self.cfg.client_id}")

    async def disconnect(self) -> None:
        if self._ib is None:
            return

        try:
            self._detach_handlers()
        finally:
            self._ib.disconnect()
            self._ib = None

    async def run_forever(self) -> None:
        backoff_s = 0.5
        max_backoff_s = 20.0

        while True:
            try:
                await self.connect()
                backoff_s = 0.5

                while self._ib is not None and self._ib.isConnected():
                    await asyncio.sleep(0.5)

                raise RuntimeError("ibkr disconnected")

            except asyncio.CancelledError:
                await self.disconnect()
                raise
            except Exception as e:
                print(f"[ibkr] disconnect err={type(e).__name__}: {e}")
                await self.disconnect()
                await asyncio.sleep(min(max_backoff_s, backoff_s))
                backoff_s *= 1.8

    async def place_limit(self, it: OrderIntent, *, tif: str = "DAY", outside_rth: bool = False) -> int:
        if self._ib is None or not self._ib.isConnected():
            raise RuntimeError("ibkr not connected")

        ib_mod = self._import_ib_async()

        symbol = it.symbol.upper()
        contract = ib_mod.Stock(symbol, self.cfg.default_exchange, self.cfg.default_currency)

        order = ib_mod.LimitOrder(
            action=it.side.upper(),
            totalQuantity=it.qty,
            lmtPrice=it.price,
            tif=tif,
            outsideRth=outside_rth,
            orderRef=it.intent_id,
            account=self.cfg.account or None,
        )

        trade = self._ib.placeOrder(contract, order)
        order_id = int(getattr(order, "orderId", 0) or getattr(trade.order, "orderId", 0) or 0)
        if order_id:
            self._orderid_to_intent[order_id] = it.intent_id
        return order_id

    def _attach_handlers(self) -> None:
        if self._ib is None:
            return

        # ib_async/ib_insync-style event hooks
        if hasattr(self._ib, "orderStatusEvent"):
            self._ib.orderStatusEvent += self._on_order_status
        if hasattr(self._ib, "execDetailsEvent"):
            self._ib.execDetailsEvent += self._on_exec_details

    def _detach_handlers(self) -> None:
        if self._ib is None:
            return

        if hasattr(self._ib, "orderStatusEvent"):
            self._ib.orderStatusEvent -= self._on_order_status
        if hasattr(self._ib, "execDetailsEvent"):
            self._ib.execDetailsEvent -= self._on_exec_details

    def _resolve_intent_id(self, trade: Any, fallback_order_id: int) -> str:
        order = getattr(trade, "order", None)
        order_ref = getattr(order, "orderRef", "") if order is not None else ""
        if order_ref:
            return str(order_ref)

        if fallback_order_id and fallback_order_id in self._orderid_to_intent:
            return self._orderid_to_intent[fallback_order_id]

        # Last-resort synthetic id for externally-created/manual orders.
        if fallback_order_id:
            sid = f"ibkr-{fallback_order_id}"
            self._orderid_to_intent[fallback_order_id] = sid
            return sid

        return "ibkr-unknown"

    def _on_order_status(self, trade: Any) -> None:
        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        if order_status is None:
            return

        raw_status = str(getattr(order_status, "status", ""))
        mapped = _status_to_order_event(raw_status)
        if not mapped:
            return

        order_id = int(getattr(order, "orderId", 0) or getattr(order_status, "orderId", 0) or 0)
        perm_id = getattr(order, "permId", 0) or getattr(order_status, "permId", 0) or order_id
        exch_id = str(perm_id) if perm_id else str(order_id)

        intent_id = self._resolve_intent_id(trade, order_id)
        reason = str(getattr(order_status, "whyHeld", "") or "")

        self.on_event(
            OrderEvent(
                type="order",
                intent_id=intent_id,
                exchange_order_id=exch_id,
                status=mapped,
                ts_ns=time.time_ns(),
                reason=reason if mapped == "REJECTED" else "",
            )
        )

    def _on_exec_details(self, trade: Any, fill: Any) -> None:
        execution = getattr(fill, "execution", None)
        if execution is None:
            return

        shares = float(getattr(execution, "shares", 0.0) or 0.0)
        price = float(getattr(execution, "price", 0.0) or 0.0)
        if shares <= 0:
            return

        order = getattr(trade, "order", None)
        order_id = int(getattr(order, "orderId", 0) or getattr(execution, "orderId", 0) or 0)
        perm_id = getattr(order, "permId", 0) or getattr(execution, "permId", 0) or order_id
        exch_id = str(perm_id) if perm_id else str(order_id)

        intent_id = self._resolve_intent_id(trade, order_id)

        ts_ns = _to_ns_from_time_like(getattr(execution, "time", None))

        self.on_event(
            FillEvent(
                type="fill",
                intent_id=intent_id,
                exchange_order_id=exch_id,
                qty=shares,
                price=price,
                ts_ns=ts_ns,
            )
        )

    @staticmethod
    def _import_ib_async() -> Any:
        try:
            import ib_async as ib_mod
        except Exception as e:  # pragma: no cover - exercised in runtime environments
            raise RuntimeError(
                "ib_async is required for IbkrIbAsyncGateway. Install dependency and retry."
            ) from e
        return ib_mod

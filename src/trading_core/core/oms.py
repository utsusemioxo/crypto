from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Set, List, Tuple


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """
    Strategy -> OMS.
    This is "I want to place an order" (not yet accepted by exchange)
    """

    type: str
    intent_id: str
    symbol: str
    side: str  # "buy" / "sell"
    qty: float
    price: float
    ts_ns: int


@dataclass(frozen=True, slots=True)
class OrderEvent:
    """
    ExecGateway -> OMS
    Exchange lifecycle signals about an order.
    """

    type: str
    intent_id: str
    exchange_order_id: str
    status: str  # "ACK" / "RECECT" / "CANCELED"
    ts_ns: int
    reason: str = ""


@dataclass(frozen=True, slots=True)
class FillEvent:
    """
    ExecGateway -> OMS.
    Trade fills for an order.
    """

    type: str
    intent_id: str
    exchange_order_id: str
    qty: float
    price: float
    ts_ns: int


class OrderStatus(str, Enum):
    NEW = "NEW"
    ACK = "ACK"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class OrderRecord:
    intent_id: str
    symbol: str = ""
    side: str = ""
    qty: float = 0.0
    price: float = 0.0

    exchange_order_id: str = ""
    status: OrderStatus = OrderStatus.NEW

    filled_qty: float = 0.0
    avg_fill_px: float = 0.0

    # Idempotency: remember processed external events (ack/fill/cancel/reject)
    processed: Set[Tuple] = field(default_factory=set)


def _update_avg_px(
    avg_px: float, filled_qty: float, new_qty: float, new_px: float
) -> float:
    # volume-weighted average price
    notional = avg_px * filled_qty + new_px * new_qty
    total = filled_qty + new_qty
    return notional / total if total > 0 else 0.0


@dataclass(slots=True)
class OMS:
    """
    OMS = single source of truth for order lifecycle.

    Key properties:
    - intent_id -> exchange_order_id mapping
    - state machine transitions
    - idempotent processing (duplicates don't double count)
    - out-of-order tolerance (e.g., fill arrives before ack)
    """

    by_intent: Dict[str, OrderRecord] = field(default_factory=dict)
    by_exch: Dict[str, str] = field(
        default_factory=dict
    )  # exchange_order_id -> intent_id

    def on_intent(self, it: OrderIntent) -> None:
        """
        Strategy submits an intent. OMS creates local NEW record.
        (Sending to exchange happens in M5; for M4 we only store.)
        """
        if it.intent_id in self.by_intent:
            # Duplicate intent submit: treat as idempotent no-op.
            return

        self.by_intent[it.intent_id] = OrderRecord(
            intent_id=it.intent_id,
            symbol=it.symbol,
            side=it.side,
            qty=it.qty,
            price=it.price,
            status=OrderStatus.NEW,
        )

    def on_order_event(self, ev: OrderEvent):
        """
        Proecss ACK/REJECT/CANCELED
        """
        rec = self._get_or_create(ev.intent_id, ev.exchange_order_id)
        key = ("order", ev.status, ev.exchange_order_id, ev.reason)
        if key in rec.processed:
            return
        rec.processed.add(key)

        # Maintain mapping (important even for out-of-order)
        if ev.exchange_order_id:
            rec.exchange_order_id = ev.exchange_order_id
            self.by_exch[ev.exchange_order_id] = rec.intent_id

        if ev.status == "ACK":
            # If already terminal (e.g., fill arrived first and completed), ignore ACK.
            if rec.status in (
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
            ):
                return
            rec.status = OrderStatus.ACK

        elif ev.status == "REJECTED":
            # if already has fills, keep conservative (ignore)
            if rec.filled_qty > 0:
                return
            rec.status = OrderStatus.REJECTED

        elif ev.status == "CANCELED":
            # If already filled, ignore cancel.
            if rec.status == OrderStatus.FILLED:
                return
            rec.status = OrderStatus.CANCELED

        else:
            raise ValueError(f"Unknown OrderEvent.status: {ev.status}")

    def on_fill(self, ev: FillEvent) -> None:
        """
        Process fills. Must be idempotent and safe under duplicates/out-of-order.
        """
        rec = self._get_or_create(ev.intent_id, ev.exchange_order_id)

        # Idempotency key: (exchange_order_id, qty, price, ts_ns)
        # In real life you'd use trade_id if available; M4 uses this stable tuple.
        key = ("fill", ev.exchange_order_id, ev.qty, ev.price, ev.ts_ns)
        if key in rec.processed:
            return
        rec.processed.add(key)

        if ev.exchange_order_id:
            rec.exchange_order_id = ev.exchange_order_id
            self.by_exch[ev.exchange_order_id] = rec.intent_id

        # Terminal guards
        if rec.status in (OrderStatus.CANCELED, OrderStatus.REJECTED):
            # Conservative: ignore fills after cancel/reject (or log anomaly).
            return

        # Apply fill
        prev_filled = rec.filled_qty
        rec.avg_fill_px = _update_avg_px(
            rec.avg_fill_px, rec.filled_qty, ev.qty, ev.price
        )
        rec.filled_qty += ev.qty

        # Update status
        # If qty not known (0), we still treat first fill as PARTIAL then maybe FILLED if exact.
        if rec.qty > 0 and rec.filled_qty >= rec.qty:
            rec.filled_qty = rec.qty  # clamp
            rec.status = OrderStatus.FILLED
        else:
            # If we were NEW and got fill first, move to PARTIAL (out-of-order tolerated).
            if prev_filled == 0.0:
                rec.status = OrderStatus.PARTIAL
            elif rec.status != OrderStatus.FILLED:
                rec.status = OrderStatus.PARTIAL

    # --- helper function ---
    def _get_or_create(self, intent_id: str, exchange_order_id: str) -> OrderRecord:
        # prefer existing by intent_id
        rec = self.by_intent.get(intent_id)
        if rec is not None:
            return rec

        # fallback: if intent_id missing/unknown but exchange_order_id maps
        if exchange_order_id and exchange_order_id in self.by_exch:
            mapped_intent = self.by_exch[exchange_order_id]
            return self.by_intent[mapped_intent]

        # Create placeholder record (happens under out-of-order events)
        rec = OrderRecord(
            intent_id=intent_id,
            exchange_order_id=exchange_order_id,
            status=OrderStatus.NEW,
        )
        self.by_intent[intent_id] = rec
        if exchange_order_id:
            self.by_exch[exchange_order_id] = intent_id
        return rec

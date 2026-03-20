from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from trading_core.core.oms import OrderIntent, OrderEvent, OMS


@dataclass(slots=True)
class TokenBucket:
    """
    Token bucket rate limiter.

    - capacity: max tokens in the bucket (allows burst)
    - refill_rate: tokens per second
    - tokens: current tokens
    - last_ts_ns: last refill timestamp in ns

    Rule:
    - each order consumes `cost` tokens (default 1)
    - tokens refill over time, capped by capacity
    """

    capacity: float
    refill_rate: float
    tokens: float = field(default=0.0)
    last_ts_ns: int = field(default=0)

    def __post_init__(self) -> None:
        # Start full so you can place a few orders immediately.
        self.tokens = float(self.capacity)

    def _refill(self, now_ns: int) -> None:
        """Update token only based on how much time has passed."""
        if self.last_ts_ns == 0:
            self.last_ts_ns = now_ns
            return

        dt_ns = now_ns - self.last_ts_ns
        if dt_ns <= 0:
            return

        dt_s = dt_ns / 1e9
        self.tokens = min(
            self.capacity, self.tokens + dt_s * self.refill_rate
        )  # The number of tokens cannot exceed capacity
        self.last_ts_ns = now_ns

    def allow(self, now_ns: int, cost: float = 1.0) -> bool:
        """
        Return True if allowed (and consume tokens), else False.
        """
        self._refill(now_ns)
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """
    M6 risk config.
    - max_position: absolute net position limit per symbol (e.g. 0.01 BTC)
    - max_order_qty: per-order qty limit
    - rate_capacity: token bucket capacity (burst)
    - rate_per_sec: token bucket refill rate (order/sec)
    """

    max_position: float
    max_order_qty: float
    rate_capacity: float
    rate_per_sec: float


@dataclass(slots=True)
class RiskEngine:
    """
    RiskEngine sits between Strategy and Execution

    Input: OrderIntent
    Output:
      - None => allowed (caller forwards intent to execution)
      - OrderEvent(REJECTED) => blocked (caller must publish so it gets recorded & replayed)

    Design:
    - Position is derived from OMS fills (determinstic/replayable).
    - No networking here. No async here. Pure decision logic.
    """

    cfg: RiskConfig
    oms: OMS
    limiter: TokenBucket = field(init=False)

    def __post_init__(self) -> None:
        self.limiter = TokenBucket(
            capacity=self.cfg.rate_capacity,
            refill_rate=self.cfg.rate_per_sec,
        )

    def check(self, it: OrderIntent) -> Optional[OrderIntent]:
        """
        Check an intent. Return REJECT event if blocked, else None.
        """
        # ---- Basic validation (defensive) ----
        if it.qty <= 0:
            return self._reject(it, "bad_qty")
        if it.side not in ("buy", "sell"):
            return self._reject(it, f"bad_side:{it.side}")
        if not it.symbol:
            return self._reject(it, "bad_symbol")

        # ---- 1) Per-order qty limit ----
        if it.qty > self.cfg.max_order_qty:
            return self._reject(
                it, f"max_order_qty_exceeded:{it.qty}>{self.cfg.max_order_qty}"
            )

        # ---- 2) Rate limit (token bucket) ----
        # Use intent ts_ns as the time source to keep behavior replay-friendly.
        if not self.limiter.allow(it.ts_ns, cost=1.0):
            return self._reject(it, "rate_limited")

        # ---- 3) Position limit (projected) ----
        cur = self._net_position_from_oms(it.symbol.lower())
        projected = cur + it.qty if it.side == "buy" else cur - it.qty

        if abs(projected) > self.cfg.max_position:
            return self._reject(it, f"max_position_exceeded:proj={projected} cur={cur}")

        return None

    def _net_position_from_oms(self, symbol: str) -> float:
        """
        Compute net position from OMS records.

        Convention:
        - buy fills add +qty
        - sell fills add -qty

        Assumes OMS records have fields:
        - symbol (str)
        - side (str)
        - filled_qty (float)
        """
        pos = 0.0
        for rec in self.oms.by_intent.values():
            if getattr(rec, "symbol", "").lower() != symbol:
                continue

            filled = float(getattr(rec, "filled_qty", 0.0))
            if filled <= 0.0:
                continue

            side = getattr(rec, "side", "")
            if side == "buy":
                pos += filled
            elif side == "sell":
                pos -= filled

        return pos

    def _reject(self, it: OrderIntent, reason: str) -> OrderEvent:
        """
        Risk rejection must be an EVENT so it can be recorded/replayed.
        """
        return OrderEvent(
            type="order",
            intent_id=it.intent_id,
            exchange_order_id="",
            status="REJECTED",
            ts_ns=it.ts_ns,
            reason=f"risk:{reason}",
        )

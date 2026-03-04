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
        self.tokens = min(self.capacity, self.tokens + dt_s * self.refill_rate) # The number of tokens cannot exceed capacity
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
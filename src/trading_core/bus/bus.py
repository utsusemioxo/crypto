from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from typing import Callable, Deque, Dict, List, Protocol, Any

class Event(Protocol):
    # Event json: {"type": "..."}
    type: str

Handler = Callable[[Event], None]

@dataclass(slots=True)
class EventBus:
    """
    Single-threaded event loop + in-memory queue.

    - publish(ev): enqueue
    - subscribe(event_type, handler): register handler for that type
    - run_forever(): pop -> dispatch to handlers (single thread)
    """
    max_queue: int = 100_000

    _q: Deque[Event] = field(default_factory=deque, init=False, repr=False)
    _handlers: Dict[str, List[Handler]] = field(default_factory=dict, init=False, repr=False)
    _dropped: int = field(default=0, init=False)

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers.setdefault(event_type, []).append(handler)

    def publish(self, ev: Event) -> None:
        # Minimal backpressure policy for M3:
        # drop newest when queue is full. (upgrade later)
        if len(self._q) >= self.max_queue:
            self._dropped += 1
            return
        self._q.append(ev)

    def run_once(self) -> int:
        """
        Processes at most one event. Returns number of events processed (0 or 1).
        """
        if not self._q:
            return 0
        
        ev = self._q.popleft()
        handlers = self._handlers.get(getattr(ev, "type", ""), [])
        for h in handlers:
            h(ev)
        return 1

    def run_batch(self, n: int = 1000) -> int:
        """
        Process up to n events. Returns processed count.
        """
        processed = 0
        for _ in range(n):
            if not self.run_once():
                break
            processed += 1
        return processed
    
    def status(self) -> dict[str, Any]:
        return {
            "queue_len": len(self._q),
            "dropped": self._dropped,
            "handlers": {k: len(v) for k, v in self._handlers.items()},
        }
        
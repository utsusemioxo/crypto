from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

def ns_to_ms(ns: int) -> float:
    return ns / 1e6


@dataclass(slots=True)
class RollingQuantiles:
    """
    Keep a rolling window of latency samples and compute p50/p99/max.

    This is intentionally simple:
    - deterministic
    - easy to debug
    - good enough for M7 visibility
    """
    capacity: int = 20_000
    xs: List[int] = field(default_factory=list)

    def add(self, x: int) -> None:
        if x <= 0:
            return
        self.xs.append(x)
        if len(self.xs) > self.capacity:
            del self.xs[0: len(self.xs) - self.capacity]
    
    def stats(self) -> Tuple[int, int, int, int]:
        """
        Return:
          (count, p50_ns, p99_ns, max_ns)
        """
        n = len(self.xs)
        if n == 0:
            return 0, 0, 0, 0
        
        s = sorted(self.xs)
        p50 = s[int(0.50 * (n - 1))]
        p99 = s[int(0.99 * (n - 1))]
        max = s[-1]
        return n, p50, p99, max

@dataclass(slots=True)
class TraceRecord:
    """
    Timeline for one trading intent.
    """
    intent_id: str
    
    # Optional market trigger timestamp
    ts_tick_ns: int = 0
    
    # Strategy decision timestamp (usually OrderIntent.ts_ns)
    ts_decision_ns: int = 0

    # Local send timestamps (before/after REST call)
    ts_send_start_ns: int = 0
    ts_send_done_ns: int = 0

    # Exchange/user-stream timestamps
    ts_ack_ns: int = 0
    ts_first_fill_ns: int = 0
    ts_last_fill_ns: int = 0

@dataclass(slots=True)
class LatencyTracker:
    """
    Track end-to-end and segment latencies

    You will feed this from:
    - MarketEvent (optional, only if you can correlate tick -> intent)
    - OrderIntent
    - OrderEvent
    - FillEvent

    And you will call:
    - mark_send_start()
    - mark_send_done()
    around REST placement in M5.
    """
    traces: Dict[str, TraceRecord] = field(default_factory=dict)

    # Segment distributions
    tick_to_decision: RollingQuantiles = field(default_factory=lambda: RollingQuantiles(20_000))
    decision_to_send: RollingQuantiles = field(default_factory=lambda: RollingQuantiles(20_000))
    send_to_ack: RollingQuantiles = field(default_factory=lambda: RollingQuantiles(20_000))
    ack_to_fill: RollingQuantiles = field(default_factory=lambda: RollingQuantiles(20_000))
    decision_to_fill: RollingQuantiles = field(default_factory=lambda: RollingQuantiles(20_000)) 

    spike_threshold_ms: float = 200.0

    def on_intent(self, it) -> None:
        """
        Record decison timestamp.
        """
        tr = self.traces.get(it.intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=it.intent_id)
            self.traces[it.intent_id] = tr
        
        tr.ts_decision_ns = it.ts_ns

        if tr.ts_tick_ns and tr.ts_decision_ns:
            self.tick_to_decision.add(tr.ts_decision_ns - tr.ts_tick_ns)
    
    def mark_tick_for_intent(self, intent_id: str, ts_tick_ns: int) -> None:
        """
        Optional helper if your strategy knows which market tick triggered this intent.
        """
        tr = self.traces.get(intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=intent_id)
            self.traces[intent_id] = tr
        
        tr.ts_tick_ns = ts_tick_ns

        if tr.ts_tick_ns and tr.ts_decision_ns:
            self.tick_to_decision.add(tr.ts_decision_ns - tr.ts_tick_ns)

    def mark_send_start(self, intent_id: str, ts_ns: int) -> None:
        tr = self.traces.get(intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=intent_id)
            self.traces[intent_id] = tr
        
        tr.ts_send_start_ns = ts_ns

        if tr.ts_decision_ns and tr.ts_send_start_ns:
            self.decision_to_send.add(tr.ts_send_start_ns - tr.ts_decision_ns)
    
    def mark_send_done(self, intent_id: str, ts_ns: int) -> None:
        tr = self.traces.get(intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=intent_id)
            self.traces[intent_id] = tr

        tr.ts_send_done_ns = ts_ns
    
    def on_order_event(self, ev) -> None:
        """
        Use ACK as the start of exchange acceptance.
        """
        tr = self.traces.get(ev.intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=ev.intent_id)
            self.traces[ev.intent_id] = tr
        
        if ev.status == "ACK" and tr.ts_ack_ns == 0:
            tr.ts_ack_ns = ev.ts_ns

            base = tr.ts_send_done_ns or tr.ts_send_start_ns
            if base and tr.ts_ack_ns:
                self.send_to_ack.add(tr.ts_ack_ns - base)
        
    def on_fill(self, ev) -> None:
        tr = self.traces.get(ev.intent_id)
        if tr is None:
            tr = TraceRecord(intent_id=ev.intent_id)
            self.traces[ev.intent_id] = tr

        if tr.ts_first_fill_ns == 0:
            tr.ts_first_fill_ns = ev.ts_ns

            if tr.ts_ack_ns:
                self.ack_to_fill.add(tr.ts_first_fill_ns - tr.ts_ack_ns)
            
            if tr.ts_decision_ns:
                total = tr.ts_first_fill_ns - tr.ts_decision_ns
                self.decision_to_fill.add(total)

                if ns_to_ms(total) >= self.spike_threshold_ms:
                    self._print_spike(tr)
    
    def _print_spike(self, tr: TraceRecord) -> None:
        """
        Print a single breakdown line for latency spikes.
        """
        total = (tr.ts_first_fill_ns - tr.ts_decision_ns) if (tr.ts_first_fill_ns and tr.ts_decision_ns) else 0
        tick_to_decision = (tr.ts_decision_ns - tr.ts_tick_ns) if (tr.ts_decision_ns and tr.ts_tick_ns) else 0
        decision_to_send = (tr.ts_send_start_ns - tr.ts_decision_ns) if (tr.ts_send_start_ns and tr.ts_decision_ns) else 0
        send_to_ack = (tr.ts_ack_ns - (tr.ts_send_done_ns or tr.ts_send_start_ns)) if (tr.ts_ack_ns and (tr.ts_send_done_ns or tr.ts_send_start_ns)) else 0
        ack_to_fill = (tr.ts_first_fill_ns - tr.ts_ack_ns) if (tr.ts_first_fill_ns and tr.ts_ack_ns) else 0
    
        print(
            "[m7][SPIKE] "
            f"intent_id={tr.intent_id} "
            f"total={ns_to_ms(total):.1f}ms "
            f"tick->decision={ns_to_ms(tick_to_decision):.1f}ms "
            f"decision->send={ns_to_ms(decision_to_send):.1f}ms "
            f"send->ack={ns_to_ms(send_to_ack):.1f}ms "
            f"ack->fill={ns_to_ms(ack_to_fill):.1f}ms"
        )

@dataclass(slots=True)
class MetricsReporter:
    """
    Periodically print production-style latency metrics.
    """
    tracker: LatencyTracker
    interval_sec: float = 5.0

    async def run_forever(self) -> None:
        import asyncio

        while True:
            await asyncio.sleep(self.interval_sec)
            self.print_metrics()

    def print_metrics(self) -> None:
        def fmt(q: RollingQuantiles) -> str:
            n, p50, p99, max = q.stats()
            return f"n={n:<6} p50={ns_to_ms(p50):6.1f}ms p99={ns_to_ms(p99):6.1f}ms max={ns_to_ms(max):6.1f}ms"
        
        print("\n==================== LATENCY ====================\n")
        print("tick->decision ", fmt(self.tracker.tick_to_decision))
        print("decision->send ", fmt(self.tracker.decision_to_send))
        print("send->ack      ", fmt(self.tracker.send_to_ack))
        print("ack->fill      ", fmt(self.tracker.ack_to_fill))
        print("decision->fill ", fmt(self.tracker.decision_to_fill))
        print("\n=================================================\n")
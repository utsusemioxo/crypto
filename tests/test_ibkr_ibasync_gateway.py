from __future__ import annotations

from datetime import datetime, timezone

from trading_core.gateway.ibkr_ibasync_gateway import (
    _status_to_order_event,
    _to_ns_from_time_like,
)


def test_status_mapping_known_values():
    assert _status_to_order_event("Submitted") == "ACK"
    assert _status_to_order_event("PendingSubmit") == "ACK"
    assert _status_to_order_event("Cancelled") == "CANCELED"
    assert _status_to_order_event("Filled") == "FILLED"
    assert _status_to_order_event("Inactive") == "REJECTED"


def test_status_mapping_unknown_is_none():
    assert _status_to_order_event("UnknownFutureStatus") is None


def test_to_ns_from_numeric_seconds():
    assert _to_ns_from_time_like(1.25) == 1_250_000_000


def test_to_ns_from_datetime():
    dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert _to_ns_from_time_like(dt) == int(dt.timestamp() * 1_000_000_000)

import pytest
from trading_core.core.risk import TokenBucket


def test_token_bucket_initialization():
    """
    Test correct initialization of TokenBucket properties.

    Verifies:
    1. Bucket starts full (tokens == capacity)
    2. Initial timestamp is set to 0
    3. Capacity and refill rate parameters are preserved
    """
    bucket = TokenBucket(capacity=10.0, refill_rate=2.0)

    assert bucket.tokens == pytest.approx(10.0)
    assert bucket.last_ts_ns == 0
    assert bucket.capacity == 10.0
    assert bucket.refill_rate == 2.0


def test_token_bucket_refill_logic():
    """
    Test core token refill mechanics of the _refill method.

    Key scenarios testbed:
    1. First call initializes timestamp only (no token refill)
    2. Tokens refill proportionally to elapsed time (capped at capacity)
    3. No refill/update if timestamp is non-increasing (time rollback protection)
    """
    bucket = TokenBucket(capacity=10.0, refill_rate=2.0)  # 2 tokens per second

    # Scenario 1: First refill call (initialize timestamp only)
    bucket._refill(now_ns=1_000_000_000)  # 1 second (1e9 nanoseconds)
    assert bucket.last_ts_ns == 1_000_000_000
    assert bucket.tokens == pytest.approx(10.0)  # Bucket full - no change

    # Scenario 2: Refill after partial token consumption
    bucket.tokens -= 5.0  # Manually consume 5 tokens (5 remaining)
    bucket._refill(now_ns=3_000_000_000)  # 2 seconds elapsed
    # Refill calculation: 2s * 2 tokens/s = 4 tokens -> 5 + 4 = 9
    assert bucket.tokens == pytest.approx(9.0)
    assert bucket.last_ts_ns == 3_000_000_000

    # Scenario 3: Refill capped at maximum capacity
    bucket._refill(now_ns=6_000_000_000)  # 3 seconds elapsed -> 3*2=6 tokens
    # 9 + 6 = 15 -> capped at capacity (10)
    assert bucket.tokens == pytest.approx(10.0)

    # Scenario 4: No refill for non-increasing timestamp (time rollback)
    bucket._refill(now_ns=5_000_000_000)  # Timestamp < last refill time
    assert bucket.tokens == pytest.approx(10.0)  # No token change
    assert bucket.last_ts_ns == 6_000_000_000  # Timestamp unchanged


def test_token_bucket_allow_basic():
    """
    Test core allow() method behavior for order rate limiting.

    Validates:
    1. Allows requests when tokens are sufficient (consumes tokens)
    2. Rejects requests when tokens are exhausted
    3. Restores allowance after token refill (time progression)
    """
    # Initialize: 5 token capacity, 1 token per second refill rate
    bucket = TokenBucket(capacity=5.0, refill_rate=1.0)

    # Scenario 1: Allowed request (sufficient tokens)
    assert bucket.allow(now_ns=1_000_000_000) is True
    assert bucket.tokens == pytest.approx(4.0)

    # Scenario 2: Exhaust all tokens with consecutive requests
    for _ in range(4):
        assert bucket.allow(now_ns=1_000_000_000) is True
    assert bucket.tokens == pytest.approx(0.0)

    # Scenario 3: Rejected request (no tokens left)
    assert bucket.allow(now_ns=1_000_000_000) is False
    assert bucket.tokens == pytest.approx(0.0)

    # Scenario 4: Restored allowance after time-based refill
    # 2 seconds elapsed -> 2 tokens refilled
    assert bucket.allow(now_ns=3_000_000_000) is True  # Consume 1 token (1 left)
    assert bucket.tokens == pytest.approx(1.0)
    assert bucket.allow(now_ns=3_000_000_000) is True  # Consume remaining token
    assert bucket.allow(now_ns=3_000_000_000) is False  # Rejected (0 tokens left)


def test_token_bucket_allow_custom_cost():
    """
    Test allow() method with custom token costs (non-default consumption).

    Validates:
    1. Requests with custom integer token costs
    2. Requests with fractional token costs
    3. Rejection when cost exceeds available tokens
    """
    bucket = TokenBucket(capacity=5.0, refill_rate=1.0)

    # Scenario 1: Allowed request with custom integer cost (2 tokens)
    assert bucket.allow(now_ns=1_000_000_000, cost=2.0) is True
    assert bucket.tokens == pytest.approx(3.0)

    # Scenario 2: Rejected request (cost > available tokens)
    assert bucket.allow(now_ns=1_000_000_000, cost=4.0) is False
    assert bucket.tokens == pytest.approx(3.0)  # No token consumption

    # Scenario 3: Allowed request with fractional cost (0.5 tokens)
    assert bucket.allow(now_ns=1_000_000_000, cost=0.5) is True
    assert bucket.tokens == pytest.approx(2.5)


def test_token_bucket_edge_cases():
    """
    Test edge cases for extreme operating conditions.

    Validates:
    1. Zero-capacity bucket (always rejects requests)
    2. Zero-refill rate (permanent rejection after token exhaustion)
    3. First allow() call (proper timestamp initialization)
    """
    # Scenario 1: Zero-capacity bucket (block all requests)
    bucket_zero_cap = TokenBucket(capacity=0.0, refill_rate=1.0)
    assert bucket_zero_cap.allow(now_ns=1_000_000_000) is False

    # Scenario 2: Zero-refill rate (no token recovery after exhaustion)
    bucket_zero_refill = TokenBucket(capacity=3.0, refill_rate=0.0)
    # Exhaust all tokens
    assert bucket_zero_refill.allow(now_ns=1_000_000_000) is True
    assert bucket_zero_refill.allow(now_ns=1_000_000_000) is True
    assert bucket_zero_refill.allow(now_ns=1_000_000_000) is True
    # No refill after 10 seconds (permanent rejection)
    assert bucket_zero_refill.allow(now_ns=11_000_000_000) is False

    # Scenario 3: First allow() call (initializes timestamp correctly)
    bucket_new = TokenBucket(capacity=5.0, refill_rate=1.0)
    assert bucket_new.allow(now_ns=1_000_000_000) is True
    assert bucket_new.tokens == pytest.approx(4.0)
    assert bucket_new.last_ts_ns == 1_000_000_000

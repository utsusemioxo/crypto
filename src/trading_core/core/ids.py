from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Final

_ALPHABET: Final[str] = "0123456789abcdefghijklmnopqrstuvwxyz"


def _base36(n: int) -> str:
    if n == 0:
        return "0"
    out: list[str] = []
    while n:
        n, r = divmod(n, 36)
        out.append(_ALPHABET[r])
    return "".join(reversed(out))


@dataclass(slots=True)
class IdGen:
    """
    Lightweight id generator.

    - session: stable prefix (helps debugging & correlation)
    - counter: monotonic increasing counter
    """

    session: str
    counter: int = 0

    @classmethod
    def new_session(cls, prefix: str = "sess") -> "IdGen":
        # small random suffix for uniqueness
        suffix = secrets.token_hex(3)  # 6 hex chars
        return cls(session=f"{prefix}-{suffix}")

    def next_intent_id(self) -> str:
        self.counter += 1
        return f"{self.session}-i{_base36(self.counter)}"

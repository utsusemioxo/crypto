from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import secrets
from typing import Protocol

class JsonSerializable(Protocol):
    def to_json(self) -> str: ...

def new_session_id(prefix: str = "sess") -> str:
    # Short unique id, good enough for local dev and replay grouping.
    return f"{prefix}-{secrets.token_hex(4)}"

@dataclass(slots=True)
class NdjsonRecorder:
    """
    Append-only ndjson recorder.

    - One event per line (JSON object)
    - Injects `session_id` into every written record.
    """
    path: Path
    session_id: str = field(default_factory=new_session_id)
    flush_each: bool = True

    _fp: object | None = field(default=None, init=False, repr=False)

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # append-only
        self._fp = self.path.open("a", encoding="utf-8")

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def append(self, ev: JsonSerializable) -> None:
        """
        Append one event.

        NOTE: We intentionally treat this as a *best-effort* recorder in M2.
        `flush_each=True` makes it eaiser to tail/inspect while running.
        """
        if self._fp is None:
            raise RuntimeError("Recorder not opened. Call open() first.")

        # event json -> dict -> inject session_id -> dump as compact json line
        d = json.loads(ev.to_json())
        d["session_id"] = self.session_id

        self._fp.write(json.dumps(d, separators=(",", ":")) + "\n")
        if self.flush_each:
            self._fp.flush()

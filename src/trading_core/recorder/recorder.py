from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
import os

class JsonSerializable(Protocol):
    def to_json(self) -> str: ...

@dataclass(slots=True)
class NdjsonRecorder:
    """
    Append-only ndjson recorder.

    - Uses line-buffered writes to reduce data loss on crash.
    - Keeps hot path simple: just append event JSON + newline.
    """
    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: JsonSerializable) -> None:
        # keep it minimal and predictable
        line = event.to_json() + "\n"
        with self.path.open("a", encoding="utf-8", buffering=1) as f:
            f.write(line)
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict

import json

from trading_core.core.oms import OMS, OrderRecord, OrderStatus
from trading_core.core.risk import RiskConfig, RiskEngine

def _to_jsonable(x: Any) -> Any:
    if isinstance(x, OrderStatus):
        return x.value
    
    if is_dataclass(x):
        d = asdict(x)
        return {k: _to_jsonable(v) for k, v in d.items()}
    
    if isinstance(x, dict):
        return {k: _to_jsonable(v) for k, v in x.items()}
    
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(v) for v in x]

    return x
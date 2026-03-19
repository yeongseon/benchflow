from __future__ import annotations

from pathlib import Path

import yaml

from .schema import Scenario


def load_scenario(path: str | Path) -> Scenario:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Scenario file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"Empty scenario file: {path}")

    return Scenario.model_validate(data)

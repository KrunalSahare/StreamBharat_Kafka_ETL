"""Loads conf/pipeline.yaml, filling ${VAR:-default} from environment variables."""
import os, re
from pathlib import Path

import yaml

_VAR = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")
CONF = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


def _expand(value):
    if isinstance(value, str):
        return _VAR.sub(lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_config(path=CONF):
    with open(path, encoding="utf-8") as f:
        return _expand(yaml.safe_load(f))

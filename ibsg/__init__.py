import json
from pathlib import Path

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

_SRC_DIR = Path(__file__).parent
_DATA_DIR = Path(__file__).parent.parent / "data"

with open(_SRC_DIR / "defaults.json") as f:
    DEFAULTS = json.load(f)

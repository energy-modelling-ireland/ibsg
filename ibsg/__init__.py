from dotenv import load_dotenv
from json import load
from os import getenv
from pathlib import Path

from ._version import get_versions

load_dotenv()

__version__ = get_versions()["version"]
del get_versions

_SRC_DIR = Path(__file__).parent
_DATA_DIR = Path(__file__).parent.parent / "data"
_LOCAL = bool(getenv("LOCAL"))

with open(_SRC_DIR / "defaults.json") as f:
    DEFAULTS = load(f)

import json
from pathlib import Path

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

_DIRPATH = Path(__file__).parent
from .defaults import DEFAULTS

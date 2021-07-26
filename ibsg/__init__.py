from configparser import ConfigParser
from json import load
from pathlib import Path

import streamlit as st

from ._version import get_versions


__version__ = get_versions()["version"]
del get_versions

_SRC_DIR = Path(__file__).parent
_DATA_DIR = Path(__file__).parent.parent / "data"
# workaround from streamlit/streamlit#400
_DOWNLOAD_DIR = Path(st.__path__[0]) / "static" / "downloads"

with open(_SRC_DIR / "defaults.json") as f:
    DEFAULTS = load(f)

CONFIG = ConfigParser()
CONFIG.read(_SRC_DIR / "config.ini")

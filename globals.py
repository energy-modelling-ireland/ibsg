from json import load
from pathlib import Path
from typing import Any
from typing import Dict

import streamlit as st


def get_data_dir() -> Path:
    data_dir = Path(__file__).parent / "data"
    if not data_dir.exists():
        data_dir.mkdir()
    return data_dir


def get_streamlit_download_dir() -> Path:
    # workaround from streamlit/streamlit#400
    download_dir = Path(st.__path__[0]) / "static" / "downloads"
    if not download_dir.exists():
        download_dir.mkdir()
    return download_dir


def get_defaults() -> Dict[str, Any]:
    with open("defaults.json") as f:
        return load(f)

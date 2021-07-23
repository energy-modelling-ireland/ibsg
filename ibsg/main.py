from configparser import ConfigParser
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import Dict
from zipfile import ZipFile

import icontract
import fsspec
import pandas as pd
import streamlit as st

from ibsg import bers
from ibsg import CONFIG
from ibsg import _DATA_DIR
from ibsg import DEFAULTS
from ibsg import io
from ibsg import postcodes


def generate_building_stock(
    selections: Dict[str, Any],
    config: ConfigParser = CONFIG,
    data_dir: Path = _DATA_DIR,
    defaults: Dict[str, Any] = DEFAULTS,
) -> None:
    if selections["ber_granularity"] == "small_area":
        bers = _load_small_area_bers(
            zipped_csv=selections["small_area_bers"],
            dtype=defaults["small_areas"]["dtype"],
            mappings=defaults["small_areas"]["mappings"],
            filepath=data_dir / config["small_area_bers"]["filename"],
        )
    else:
        with fsspec.open(
            config["postcode_bers"]["url"],
            filecache={"cache_storage", data_dir / config["postcode_bers"]["filename"]},
        ) as f:
            bers = pd.read_parquet(f)


def _load_small_area_bers(
    zipped_csv: BytesIO, dtype: Dict[str, Any], mappings: Dict[str, Any], filepath: Path
) -> pd.DataFrame:
    breakpoint()
    if not filepath.exists():
        io.convert_zipped_csv_to_parquet(
            zipped_csv=zipped_csv, dtype=dtype, mappings=mappings, filepath=filepath
        )
        del zipped_csv
    return pd.read_parquet(filepath)

from configparser import ConfigParser
from io import BytesIO
from pathlib import Path
from typing import Any, List
from typing import Dict

import fsspec
import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg import _DATA_DIR
from ibsg import DEFAULTS
from ibsg import io


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
        ).pipe(_filter_bers, selections=selections, defaults=defaults)

    else:
        bers = _load_postcode_bers(
            url=config["postcode_bers"]["url"],
            filepath=data_dir / config["postcode_bers"]["filename"],
        ).pipe(_filter_bers, selections=selections, defaults=defaults)


def _load_postcode_bers(url: str, filepath: Path):
    if not filepath.exists():
        with fsspec.open(url) as f:
            pd.read_parquet(f).to_parquet(filepath)
    return pd.read_parquet(filepath)


def _load_small_area_bers(
    zipped_csv: BytesIO, dtype: Dict[str, Any], mappings: Dict[str, Any], filepath: Path
) -> pd.DataFrame:
    if not filepath.exists():
        io.convert_zipped_csv_to_parquet(
            zipped_csv=zipped_csv, dtype=dtype, mappings=mappings, filepath=filepath
        )
        del zipped_csv
    return pd.read_parquet(filepath)


def _filter_bers(
    raw_bers: pd.DataFrame, selections: Dict[str, Any], defaults: Dict[str, Any]
) -> pd.DataFrame:
    selected_bers = clean.get_rows_containing_substrings(
        df=raw_bers,
        column_name="countyname",
        selected_substrings=selections["countyname"],
        all_substrings=defaults["countyname"],
    )
    return _remove_erroneous_bers(
        bers=selected_bers,
        selected_filters=selections["filters"],
        bounds=selections["bounds"],
    )


def _remove_erroneous_bers(
    bers: pd.DataFrame, selected_filters: List[str], bounds: Dict[str, Dict[str, int]]
) -> pd.DataFrame:
    return (
        bers.pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is not provisional",
            selected_filters=selected_filters,
            condition="type_of_rating != 'Provisional    '",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="lb < ground_floor_area < ub",
            selected_filters=selected_filters,
            condition="ground_floor_area > {lb} and ground_floor_area < {ub}".format(
                lb=bounds["ground_floor_area"]["lb"],
                ub=bounds["ground_floor_area"]["ub"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="lb < living_area_percent < ub",
            selected_filters=selected_filters,
            condition="living_area_percent > {lb} or living_area_percent < {ub}".format(
                lb=bounds["living_area_percent"]["lb"],
                ub=bounds["living_area_percent"]["ub"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="lb < main_sh_boiler_efficiency < ub",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency > {lb} or main_sh_boiler_efficiency < {ub}".format(
                lb=bounds["main_sh_boiler_efficiency"]["lb"],
                ub=bounds["main_sh_boiler_efficiency"]["ub"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="lb < main_hw_boiler_efficiency < ub",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency > {lb} or main_hw_boiler_efficiency < {ub}".format(
                lb=bounds["main_hw_boiler_efficiency"]["lb"],
                ub=bounds["main_hw_boiler_efficiency"]["ub"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_sh_boiler_efficiency_adjustment_factor > lb",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency_adjustment_factor > {lb}".format(
                lb=bounds["main_sh_boiler_efficiency_adjustment_factor"]["lb"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_hw_boiler_efficiency_adjustment_factor > lb",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency_adjustment_factor > {lb}".format(
                lb=bounds["main_hw_boiler_efficiency_adjustment_factor"]["lb"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="declared_loss_factor < ub",
            selected_filters=selected_filters,
            condition="declared_loss_factor < {ub}".format(
                ub=bounds["declared_loss_factor"]["ub"],
            ),
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="lb < thermal_bridging_factor < ub",
            selected_filters=selected_filters,
            condition="thermal_bridging_factor > {lb} or thermal_bridging_factor <= {ub}".format(
                lb=bounds["thermal_bridging_factor"]["lb"],
                ub=bounds["thermal_bridging_factor"]["ub"],
            ),
        )
    )

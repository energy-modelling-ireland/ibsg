from configparser import ConfigParser
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

from zipfile import ZipFile

import icontract
import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import filter
from ibsg import io
from ibsg import _DATA_DIR

from ibsg import DEFAULTS
from ibsg import CONFIG


def main(
    zipped_csv_of_bers: BytesIO,
    selections: Dict[str, Any],
    defaults: Dict[str, Any] = DEFAULTS,
    config: ConfigParser = CONFIG,
    data_dir: Path = _DATA_DIR,
) -> pd.DataFrame:
    ## Load
    raw_sa_bers = _load_small_area_bers(zipped_csv_of_bers)
    sa_ids_2016 = _load_2016_small_area_ids(
        url=config["small_area_ids"]["url"],
        data_dir=data_dir,
        filesystem_name=config["small_area_ids"]["filesystem"],
    )

    with st.form("Apply Filters"):
        ## Filter
        sa_bers_in_countyname = filter.filter_by_substrings(
            raw_sa_bers,
            column_name="countyname",
            selected_substrings=selections["countyname"],
            all_substrings=defaults["countyname"],
        )

        ## Clean
        filtered_small_area_bers = _filter_small_area_bers(
            bers=sa_bers_in_countyname,
            small_area_ids=sa_ids_2016,
        )

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    return filtered_small_area_bers


@st.cache
@icontract.require(
    lambda zipped_csv_of_bers: len(
        [f for f in ZipFile(zipped_csv_of_bers).namelist() if "csv" in f]
    )
    == 1
)
def _load_small_area_bers(zipped_csv_of_bers: BytesIO) -> pd.DataFrame:
    dtype = DEFAULTS["small_areas"]["dtype"]
    mappings = DEFAULTS["small_areas"]["mappings"]
    zip = ZipFile(zipped_csv_of_bers)
    filename = [f for f in zip.namelist() if "csv" in f][0]
    return io.read(zip.open(filename), dtype=dtype, mappings=mappings)


@st.cache
def _load_2016_small_area_ids(
    url: str, data_dir: Path, filesystem_name: str
) -> List[str]:
    return (
        io.load(
            read=pd.read_csv,
            url=url,
            data_dir=data_dir,
            filesystem_name=filesystem_name,
        )
        .squeeze()
        .to_list()
    )


def _filter_small_area_bers(
    bers: pd.DataFrame,
    small_area_ids: List[str],
) -> pd.DataFrame:
    filter_names = [
        "Is not provisional",
        "0m² < ground_floor_area < 1000m²",
        "5% < living_area_percent < 90%",
        "main_sh_boiler_efficiency > 19%",
        "19% < main_hw_boiler_efficiency < 320%",
        "main_sh_boiler_efficiency_adjustment_factor > 0.7",
        "main_hw_boiler_efficiency_adjustment_factor > 0.7",
        "suppl_sh_boiler_efficiency_adjustment_factor > 19",
        "Is valid small area id",
    ]
    selected_filters = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
    )
    clean_bers = (
        bers.copy()
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is not provisional",
            selected_filters=selected_filters,
            condition="type_of_rating != 'P '",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="0m² < ground_floor_area < 1000m²",
            selected_filters=selected_filters,
            condition="ground_floor_area > 30 and ground_floor_area < 1000",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="5% < living_area_percent < 90%",
            selected_filters=selected_filters,
            condition="living_area_percent < 90 or living_area_percent > 5",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_sh_boiler_efficiency > 19%",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency > 19",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="19% < main_hw_boiler_efficiency < 320%",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency < 320 or main_hw_boiler_efficiency > 19",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_sh_boiler_efficiency_adjustment_factor > 0.7",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency_adjustment_factor > 0.7",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_hw_boiler_efficiency_adjustment_factor > 0.7",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency_adjustment_factor > 0.7",
        )
        .pipe(
            clean.get_rows_equal_to_values,
            filter_name="Is valid small area id",
            selected_filters=selected_filters,
            on_column="small_area",
            values=small_area_ids,
        )
    )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_change = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️ Filtering removed {round(percentage_change, 2)}% of buildings ⚠️")
    return clean_bers

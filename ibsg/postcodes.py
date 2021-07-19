from collections import defaultdict
from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg import DEFAULTS
from ibsg import filter
from ibsg import io
from ibsg import _DATA_DIR


def main(
    selections: Dict[str, Any],
    defaults: Dict[str, Any] = DEFAULTS,
    config: ConfigParser = CONFIG,
    data_dir: Path = _DATA_DIR,
) -> pd.DataFrame:
    ## Fetch
    postcode_bers_raw = _load_postcode_bers(
        read=pd.read_parquet,
        url=config["postcode_bers"]["url"],
        data_dir=data_dir,
        filesystem_name=config["postcode_bers"]["filesystem"],
    )

    with st.form("Apply Filters"):
        ## Filter
        postcode_bers_in_countyname = filter.filter_by_substrings(
            postcode_bers_raw,
            column_name="countyname",
            selected_substrings=selections["countyname"],
            all_substrings=defaults["countyname"],
        )
        clean_postcode_bers = _clean_postcode_bers(postcode_bers_in_countyname)

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    return clean_postcode_bers


@st.cache
def _load_postcode_bers(
    read: Callable, url: str, data_dir: Path, filesystem_name: str
) -> pd.DataFrame:
    return io.load(
        read=read, url=url, data_dir=data_dir, filesystem_name=filesystem_name
    )


def _clean_postcode_bers(bers: pd.DataFrame) -> pd.DataFrame:
    filter_names = [
        "Is not provisional",
        "lb < ground_floor_area < ub",
        "lb < living_area_percent < ub",
        "lb < main_sh_boiler_efficiency < ub",
        "lb < main_hw_boiler_efficiency < ub",
        "main_sh_boiler_efficiency_adjustment_factor > lb",
        "main_hw_boiler_efficiency_adjustment_factor > lb",
        "declared_loss_factor < ub",
        "lb < thermal_bridging_factor < ub",
    ]
    selected_filters = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
    )
    st.info("*lb = Lower Bound, ub = Upper Bound*")

    with st.beta_expander("Change Filter Bounds?"):
        c1, c2 = st.beta_columns(2)
        selections: Dict[str, Dict[str, str]] = defaultdict()
        selections = {
            "ground_floor_area": {
                "lb": c1.number_input("Lower Bound: ground_floor_area", value=0),
                "ub": c2.number_input("Upper Bound: ground_floor_area", value=1000),
            },
            "living_area_percent": {
                "lb": c1.number_input("Lower Bound: living_area_percent", value=5),
                "ub": c2.number_input("Lower Bound: living_area_percent", value=90),
            },
            "main_sh_boiler_efficiency": {
                "lb": c1.number_input(
                    "Lower Bound: main_sh_boiler_efficiency", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: main_sh_boiler_efficiency", value=600
                ),
            },
            "main_hw_boiler_efficiency": {
                "lb": c1.number_input(
                    "Lower Bound: main_hw_boiler_efficiency", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: main_hw_boiler_efficiency", value=320
                ),
            },
            "main_sh_boiler_efficiency_adjustment_factor": {
                "lb": st.number_input(
                    "Lower Bound: main_sh_boiler_efficiency_adjustment_factor",
                    value=0.7,
                ),
            },
            "main_hw_boiler_efficiency_adjustment_factor": {
                "lb": st.number_input(
                    "Lower Bound: main_hw_boiler_efficiency_adjustment_factor",
                    value=0.7,
                ),
            },
            "declared_loss_factor": {
                "ub": st.number_input(
                    "Upper Bound: declared_loss_factor",
                    value=20,
                ),
            },
            "thermal_bridging_factor": {
                "lb": c1.number_input("Lower Bound: thermal_bridging_factor", value=0),
                "ub": c2.number_input(
                    "Lower Bound: thermal_bridging_factor", value=0.15
                ),
            },
        }
        clean_bers = (
            bers.copy()
            .pipe(
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
                    lb=selections["ground_floor_area"]["lb"],
                    ub=selections["ground_floor_area"]["ub"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < living_area_percent < ub",
                selected_filters=selected_filters,
                condition="living_area_percent > {lb} or living_area_percent < {ub}".format(
                    lb=selections["living_area_percent"]["lb"],
                    ub=selections["living_area_percent"]["ub"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < main_sh_boiler_efficiency < ub",
                selected_filters=selected_filters,
                condition="main_sh_boiler_efficiency > {lb} or main_sh_boiler_efficiency < {ub}".format(
                    lb=selections["main_sh_boiler_efficiency"]["lb"],
                    ub=selections["main_sh_boiler_efficiency"]["ub"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < main_hw_boiler_efficiency < ub",
                selected_filters=selected_filters,
                condition="main_hw_boiler_efficiency > {lb} or main_hw_boiler_efficiency < {ub}".format(
                    lb=selections["main_hw_boiler_efficiency"]["lb"],
                    ub=selections["main_hw_boiler_efficiency"]["ub"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="main_sh_boiler_efficiency_adjustment_factor > lb",
                selected_filters=selected_filters,
                condition="main_sh_boiler_efficiency_adjustment_factor > {lb}".format(
                    lb=selections["main_sh_boiler_efficiency_adjustment_factor"]["lb"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="main_hw_boiler_efficiency_adjustment_factor > lb",
                selected_filters=selected_filters,
                condition="main_hw_boiler_efficiency_adjustment_factor > {lb}".format(
                    lb=selections["main_hw_boiler_efficiency_adjustment_factor"]["lb"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="declared_loss_factor < ub",
                selected_filters=selected_filters,
                condition="declared_loss_factor < {ub}".format(
                    ub=selections["declared_loss_factor"]["ub"],
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < thermal_bridging_factor < ub",
                selected_filters=selected_filters,
                condition="thermal_bridging_factor > {lb} or thermal_bridging_factor <= {ub}".format(
                    lb=selections["thermal_bridging_factor"]["lb"],
                    ub=selections["thermal_bridging_factor"]["ub"],
                ),
            )
        )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_change = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️ Filtering removed {round(percentage_change, 2)}% of buildings ⚠️")
    return clean_bers

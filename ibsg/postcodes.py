from configparser import ConfigParser
from pathlib import Path

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg.fetch import fetch
from ibsg import filter
from ibsg import _LOCAL
from ibsg import _DATA_DIR


def main(config: ConfigParser = CONFIG) -> pd.DataFrame:
    ## Fetch
    postcode_bers_raw = _load_postcode_bers(config["urls"]["postcode_bers"])

    with st.form("Apply Filters"):
        ## Filter
        postcode_bers_in_countyname = filter.filter_by_substrings(
            postcode_bers_raw,
            column_name="countyname",
            all_substrings=[
                "Co. Carlow",
                "Co. Cavan",
                "Co. Clare",
                "Co. Cork",
                "Co. Donegal",
                "Co. Dublin",
                "Co. Galway",
                "Co. Kerry",
                "Co. Kildare",
                "Co. Kilkenny",
                "Co. Laois",
                "Co. Leitrim",
                "Co. Limerick",
                "Co. Longford",
                "Co. Louth",
                "Co. Mayo",
                "Co. Meath",
                "Co. Monaghan",
                "Co. Offaly",
                "Co. Roscommon",
                "Co. Sligo",
                "Co. Tipperary",
                "Co. Waterford",
                "Co. Westmeath",
                "Co. Wexford",
                "Co. Wicklow",
                "Cork City",
                "Dublin 1",
                "Dublin 10",
                "Dublin 11",
                "Dublin 12",
                "Dublin 13",
                "Dublin 14",
                "Dublin 15",
                "Dublin 16",
                "Dublin 17",
                "Dublin 18",
                "Dublin 2",
                "Dublin 20",
                "Dublin 22",
                "Dublin 24",
                "Dublin 3",
                "Dublin 4",
                "Dublin 5",
                "Dublin 6",
                "Dublin 6W",
                "Dublin 7",
                "Dublin 8",
                "Dublin 9",
                "Galway City",
                "Limerick City",
                "Waterford City",
            ],
        )
        clean_postcode_bers = _clean_postcode_bers(postcode_bers_in_countyname)

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    return clean_postcode_bers


@st.cache
def _load_postcode_bers(url: str) -> pd.DataFrame:
    filepath = fetch(url, _LOCAL, _DATA_DIR)
    return pd.read_parquet(filepath)


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
                    lb=c1.number_input("Lower Bound: ground_floor_area", value=0),
                    ub=c2.number_input("Upper Bound: ground_floor_area", value=1000),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < living_area_percent < ub",
                selected_filters=selected_filters,
                condition="living_area_percent > {lb} or living_area_percent < {ub}".format(
                    lb=c1.number_input("Lower Bound: living_area_percent", value=5),
                    ub=c2.number_input("Upper Bound: living_area_percent", value=90),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < main_sh_boiler_efficiency < ub",
                selected_filters=selected_filters,
                condition="main_sh_boiler_efficiency > {lb} or main_sh_boiler_efficiency < {ub}".format(
                    lb=c1.number_input(
                        "Lower Bound: main_sh_boiler_efficiency", value=19
                    ),
                    ub=c2.number_input(
                        "Upper Bound: main_sh_boiler_efficiency", value=600
                    ),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < main_hw_boiler_efficiency < ub",
                selected_filters=selected_filters,
                condition="main_hw_boiler_efficiency > {lb} or main_hw_boiler_efficiency < {ub}".format(
                    lb=c1.number_input(
                        "Lower Bound: main_hw_boiler_efficiency", value=19
                    ),
                    ub=c2.number_input(
                        "Upper Bound: main_hw_boiler_efficiency", value=320
                    ),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="main_sh_boiler_efficiency_adjustment_factor > lb",
                selected_filters=selected_filters,
                condition="main_sh_boiler_efficiency_adjustment_factor > {lb}".format(
                    lb=st.number_input(
                        "Lower Bound: main_sh_boiler_efficiency_adjustment_factor",
                        value=0.7,
                    ),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="main_hw_boiler_efficiency_adjustment_factor > lb",
                selected_filters=selected_filters,
                condition="main_hw_boiler_efficiency_adjustment_factor > {lb}".format(
                    lb=st.number_input(
                        "Lower Bound: main_hw_boiler_efficiency_adjustment_factor",
                        value=0.7,
                    ),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="declared_loss_factor < ub",
                selected_filters=selected_filters,
                condition="declared_loss_factor < {ub}".format(
                    ub=st.number_input(
                        "Upper Bound: declared_loss_factor",
                        value=20,
                    ),
                ),
            )
            .pipe(
                clean.get_rows_meeting_condition,
                filter_name="lb < thermal_bridging_factor < ub",
                selected_filters=selected_filters,
                condition="thermal_bridging_factor > {lb} or thermal_bridging_factor <= {ub}".format(
                    lb=c1.number_input("Lower Bound: thermal_bridging_factor", value=0),
                    ub=c2.number_input(
                        "Upper Bound: thermal_bridging_factor", value=0.15
                    ),
                ),
            )
        )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_change = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️ Filtering removed {round(percentage_change, 2)}% of buildings ⚠️")
    return clean_bers

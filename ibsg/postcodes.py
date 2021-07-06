import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import filter


def main() -> pd.DataFrame:
    ## Fetch
    postcode_bers_raw = _load_postcode_bers()

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
def _load_postcode_bers() -> pd.DataFrame:
    return pd.read_parquet(
        "https://storage.googleapis.com/ibsg/BERPublicsearch.parquet"
    )


def _clean_postcode_bers(bers: pd.DataFrame) -> pd.DataFrame:
    filter_names = [
        "Is not provisional",
        "Is valid ground_floor_area",
        "Is valid living_area_percent",
        "Is valid main_sh_boiler_efficiency",
        "Is valid main_hw_boiler_efficiency",
        "Is valid main_sh_boiler_efficiency_adjustment_factor",
        "Is valid main_hw_boiler_efficiency_adjustment_factor",
        "Is valid suppl_sh_heat_fraction",
        "Is valid declared_loss_factor",
        "Is valid thermal_bridging_factor",
    ]
    selected_filters = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
    )
    with st.beta_expander("Change Filter Bounds"):
        c1, c2 = st.beta_columns(2)
        ground_floor_area_lower_bound = c1.number_input(
            "ground_floor_area_lower_bound", value=0
        )
        ground_floor_area_upper_bound = c2.number_input(
            "ground_floor_area_upper_bound", value=1000
        )
        living_area_percent_lower_bound = c1.number_input(
            "living_area_percent_lower_bound",
            value=5,
        )
        living_area_percent_upper_bound = c2.number_input(
            "living_area_percent_upper_bound",
            value=90,
        )
        main_sh_boiler_efficiency_lower_bound = c1.number_input(
            "main_sh_boiler_efficiency_lower_bound",
            value=19,
        )
        main_sh_boiler_efficiency_upper_bound = c2.number_input(
            "main_sh_boiler_efficiency_upper_bound",
            value=600,
        )
        main_hw_boiler_efficiency_lower_bound = c1.number_input(
            "main_hw_boiler_efficiency_lower_bound",
            value=19,
        )
        main_hw_boiler_efficiency_upper_bound = c2.number_input(
            "main_hw_boiler_efficiency_upper_bound",
            value=320,
        )
        main_sh_boiler_efficiency_adjustment_factor_lower_bound = st.number_input(
            "main_sh_boiler_efficiency_adjustment_factor_lower_bound",
            value=0.7,
        )
        main_hw_boiler_efficiency_adjustment_factor_lower_bound = st.number_input(
            "main_hw_boiler_efficiency_adjustment_factor_lower_bound",
            value=0.7,
        )
        declared_loss_factor_upper_bound = st.number_input(
            "declared_loss_factor_upper_bound",
            value=20,
        )
        thermal_bridging_factor_lower_bound = c1.number_input(
            "thermal_bridging_factor_lower_bound",
            value=0,
        )
        thermal_bridging_factor_upper_bound = c2.number_input(
            "thermal_bridging_factor_upper_bound",
            value=0.15,
        )
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
            filter_name="Is valid ground_floor_area",
            selected_filters=selected_filters,
            condition=f"ground_floor_area > {ground_floor_area_lower_bound}"
            + f" and ground_floor_area < {ground_floor_area_upper_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid living_area_percent",
            selected_filters=selected_filters,
            condition=f"living_area_percent > {living_area_percent_lower_bound}"
            + f" or living_area_percent < {living_area_percent_upper_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid main_sh_boiler_efficiency",
            selected_filters=selected_filters,
            condition=f"main_sh_boiler_efficiency > {main_sh_boiler_efficiency_lower_bound}"
            + f" or main_sh_boiler_efficiency < {main_sh_boiler_efficiency_upper_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid main_hw_boiler_efficiency",
            selected_filters=selected_filters,
            condition=f"main_hw_boiler_efficiency > {main_hw_boiler_efficiency_lower_bound}"
            + f" or main_hw_boiler_efficiency < {main_hw_boiler_efficiency_upper_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid main_sh_boiler_efficiency_adjustment_factor",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency_adjustment_factor"
            + f" > {main_sh_boiler_efficiency_adjustment_factor_lower_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid main_hw_boiler_efficiency_adjustment_factor",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency_adjustment_factor"
            + f" > {main_hw_boiler_efficiency_adjustment_factor_lower_bound}",
        )
        .pipe(
            clean.get_rows_equal_to_values,
            filter_name="Is valid suppl_sh_heat_fraction",
            selected_filters=selected_filters,
            on_column="suppl_sh_heat_fraction",
            values=["0", "0.1", "0.15", "0.2"],
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid declared_loss_factor",
            selected_filters=selected_filters,
            condition=f"declared_loss_factor < {declared_loss_factor_upper_bound}",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is valid thermal_bridging_factor",
            selected_filters=selected_filters,
            condition=f"thermal_bridging_factor > {thermal_bridging_factor_lower_bound}"
            + f" or thermal_bridging_factor <= {thermal_bridging_factor_upper_bound}",
        )
    )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_change = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️ Filtering removed {round(percentage_change, 2)}% of buildings ⚠️")
    return clean_bers

import datetime
import pathlib
from typing import List

import pandas as pd
import streamlit as st

from ibsg import clean


def main():
    st.header("🏠 Irish Building Stock Generator (IBSG) 🏠")
    ## Load
    sa_bers_file = st.file_uploader(label="Upload Small Area BERs", type=["csv"])
    raw_sa_bers = _load_small_area_bers(sa_bers_file)
    sa_ids_2016 = _load_small_area_ids()

    with st.form("Apply Filters"):
        ## Filter
        sa_bers_in_countyname = _filter_by_substrings(
            raw_sa_bers,
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

        ## Clean
        clean_small_area_bers = _clean_small_area_bers(
            bers=sa_bers_in_countyname,
            small_area_ids=sa_ids_2016,
        )

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    save_to_csv_selected = st.button("Save data to csv?")
    if save_to_csv_selected:
        ## Download
        _download_csv(
            df=clean_small_area_bers,
            filename=f"ibsg_buildings_{datetime.date.today()}_small_area.zip",
        )


@st.cache
def _load_small_area_bers(file) -> pd.DataFrame:
    extract_columns = [
        "small_area",
        "year_of_construction",
        "energy_value",
        "type_of_rating",
        "living_area_percent",
        "roof_area",
        "roof_uvalue",
        "wall_area",
        "wall_uvalue",
        "floor_area",
        "floor_uvalue",
        "window_area",
        "window_uvalue",
        "door_area",
        "door_uvalue",
        "ground_floor_area",
        "first_floor_area",
        "second_floor_area",
        "third_floor_area",
        "main_sh_boiler_efficiency",
        "main_hw_boiler_efficiency",
        "main_sh_boiler_efficiency_adjustment_factor",
        "main_hw_boiler_efficiency_adjustment_factor",
        "suppl_sh_boiler_efficiency_adjustment_factor",
    ]
    if file:
        bers = pd.read_csv(file)
    else:
        bers = pd.read_csv("data/BER.public.14.05.2021/BER.public.14.05.2021.csv")
    standardised_bers = bers.pipe(clean.standardise_ber_private_column_names)
    return standardised_bers[extract_columns]


@st.cache
def _load_small_area_ids() -> List[str]:
    return pd.read_csv("data/small_area_ids_2016.csv", squeeze=True).to_list()


def _clean_small_area_bers(
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
            clean.get_rows_meeting_condition,
            filter_name="suppl_sh_boiler_efficiency_adjustment_factor > 19",
            selected_filters=selected_filters,
            condition="suppl_sh_boiler_efficiency_adjustment_factor > 19",
        )
        .pipe(
            clean.get_rows_equal_to_values,
            filter_name="Is valid small area id",
            selected_filters=selected_filters,
            on_column="small_area",
            values=small_area_ids,
        )
    )
    st.write("⚠️Filtering removed" f" {len(bers) - len(clean_bers)}" " buildings!")
    return clean_bers


def _filter_by_substrings(
    df: pd.DataFrame, column_name: str, all_substrings: List[str]
) -> pd.DataFrame:
    selected_substrings = st.multiselect(
        f"Select {column_name}", all_substrings, default=all_substrings
    )
    if selected_substrings == all_substrings:
        selected_df = df
    else:
        substrings_to_search = "|".join(selected_substrings)
        selected_df = df[
            df[column_name].str.title().str.contains(substrings_to_search, regex=True)
        ]
    return selected_df


def _download_csv(df: pd.DataFrame, filename: str):
    # workaround from streamlit/streamlit#400
    STREAMLIT_STATIC_PATH = pathlib.Path(st.__path__[0]) / "static"
    DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
    if not DOWNLOADS_PATH.is_dir():
        DOWNLOADS_PATH.mkdir()
    df.to_csv(DOWNLOADS_PATH / filename, compression="zip", index=False)
    st.markdown(f"Download [{filename}](downloads/{filename})")


if __name__ == "__main__":
    main()

import datetime
import pathlib
from typing import List

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import POSTCODES
from ibsg import COUNTIES


def main():
    st.header("🏠 Irish Building Stock Generator (IBSG) 🏠")
    ## Load
    sa_bers_file = st.file_uploader(label="Upload Small Area BERs", type=["csv"])
    raw_sa_bers = _load_small_area_bers(sa_bers_file)
    sa_ids_2016 = _load_small_area_ids()

    with st.form("Apply Filters"):
        ## Filter
        sa_bers_in_counties = _filter_by_county(raw_sa_bers, COUNTIES)
        sa_bers_in_postcodes = _filter_by_postcode(raw_sa_bers, POSTCODES)
        bers_in_selected_region = min(
            [sa_bers_in_counties, sa_bers_in_postcodes], key=len
        )

        ## Clean
        clean_small_area_bers = _clean_small_area_bers(
            bers=bers_in_selected_region,
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


def _raise_for_all_query(x):
    if "All" in x:
        raise ValueError("Please deselect 'All'!")


def _filter_by_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    selected_columns = st.multiselect(
        "Select Columns", ["All"] + columns, default="All"
    )
    if selected_columns == ["All"]:
        selected_rows = df[columns]
    else:
        _raise_for_all_query(selected_columns)
        selected_rows = df[selected_columns]
    return selected_rows


def _filter_by_county(bers: pd.DataFrame, counties: List[str]) -> pd.DataFrame:
    selected_counties = st.multiselect(
        "Select Counties", ["All"] + counties, default="All"
    )
    if selected_counties == ["All"]:
        selected_bers = bers
    else:
        _raise_for_all_query(selected_counties)
        counties_to_search = "|".join(selected_counties)
        selected_bers = bers[
            bers["countyname"].str.title().str.contains(counties_to_search, regex=True)
        ]
    return selected_bers


def _filter_by_postcode(bers: pd.DataFrame, postcodes: List[str]) -> pd.DataFrame:
    selected_postcodes = st.multiselect(
        "Select Postcodes", ["All"] + postcodes, default="All"
    )
    if selected_postcodes == ["All"]:
        selected_bers = bers
    else:
        _raise_for_all_query(selected_postcodes)
        selected_bers = bers[bers["countyname"].str.title().isin(selected_postcodes)]
    return selected_bers


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

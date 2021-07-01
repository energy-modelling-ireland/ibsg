import datetime
import pathlib
from typing import List

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import POSTCODES
from ibsg import COUNTIES

OUTPUT_COLUMNS = [
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


def main():
    st.header(" 🧹 Irish Building Energy Ratings (BER)")
    ## Load
    sa_bers_file = st.file_uploader(label="Upload Small Area BERs", type=["csv"])
    raw_sa_bers = _load_small_area_bers(sa_bers_file, columns=OUTPUT_COLUMNS)
    sa_ids_2016 = _load_small_area_ids()

    with st.form("Apply Filters"):
        ## Filter
        sa_bers_in_counties = _filter_by_county(raw_sa_bers, COUNTIES)
        sa_bers_in_postcodes = _filter_by_postcode(raw_sa_bers, POSTCODES)
        filtered_bers = min([sa_bers_in_counties, sa_bers_in_postcodes], key=len)
        submit_button = st.form_submit_button(label="Apply")

    if submit_button:
        ## Clean
        clean_small_area_bers = _clean_small_area_bers(
            bers=raw_sa_bers, small_area_ids=sa_ids_2016
        )
        ## Download
        _download_csv(
            df=filtered_bers,
            filename=f"clean_small_area_bers_{datetime.date.today()}.zip",
        )


@st.cache
def _load_small_area_bers(file, columns: List[str]) -> pd.DataFrame:
    if file:
        bers = pd.read_csv(file)
    else:
        bers = pd.read_csv("data/BER.public.14.05.2021/BER.public.14.05.2021.csv")
    standardised_bers = bers.pipe(clean.standardise_ber_private_column_names)
    return standardised_bers[columns]


@st.cache
def _load_small_area_ids() -> List[str]:
    return pd.read_csv("data/small_area_ids_2016.csv", squeeze=True).to_list()


@st.cache
def _clean_small_area_bers(
    bers: pd.DataFrame,
    small_area_ids: List[str],
) -> pd.DataFrame:
    return (
        bers.pipe(clean.is_not_provisional)
        .pipe(clean.is_valid_floor_area)
        .pipe(clean.is_valid_living_area_percentage)
        .pipe(clean.is_valid_sh_boiler_efficiency)
        .pipe(clean.is_valid_hw_boiler_efficiency)
        .pipe(clean.is_valid_sh_boiler_efficiency_adjustment_factor)
        .pipe(clean.is_valid_hw_boiler_efficiency_adjustment_factor)
        .pipe(clean.is_valid_suppl_boiler_efficiency_adjustment_factor)
        .pipe(clean.is_valid_small_area_id, small_area_ids)
    )


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

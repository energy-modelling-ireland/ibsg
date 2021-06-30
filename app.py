from typing import List

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import POSTCODES
from ibsg import COUNTIES


def main():
    ## Load
    sa_bers_file = st.file_uploader(label="Upload Small Area BERs", type=["csv"])
    raw_sa_bers = _load_small_area_bers(sa_bers_file)
    sa_ids_2016 = _load_small_area_ids()

    ## Clean
    clean_small_area_bers = _clean_small_area_bers(
        bers=raw_sa_bers, small_area_ids=sa_ids_2016
    )

    ## Filter
    sa_bers_in_counties = _filter_by_county(clean_small_area_bers, COUNTIES)
    sa_bers_in_postcodes = _filter_by_county(clean_small_area_bers, POSTCODES)
    filtered_bers = min([sa_bers_in_counties, sa_bers_in_postcodes], key=len)


@st.cache
def _load_small_area_bers(file) -> pd.DataFrame:
    if file:
        bers = pd.read_csv(file.getvalue())
    else:
        bers = pd.read_csv("data/BER.public.14.05.2021/BER.public.14.05.2021.csv")
    return bers


@st.cache
def _load_small_area_ids() -> List[str]:
    return pd.read_csv("data/small_area_ids_2016.csv", squeeze=True).to_list()


@st.cache
def _clean_small_area_bers(
    bers: pd.DataFrame, small_area_ids: List[str]
) -> pd.DataFrame:
    return (
        bers.pipe(clean.standardise_ber_private_column_names)
        .pipe(clean.is_not_provisional)
        .pipe(clean.is_realistic_floor_area)
        .pipe(clean.is_realistic_living_area_percentage)
        .pipe(clean.is_realistic_boiler_efficiency)
        .pipe(clean.is_realistic_boiler_efficiency_adjustment_factor)
        .pipe(clean.is_realistic_boiler_efficiency)
        .pipe(clean.is_real_small_area, small_area_ids)
    )


def _raise_for_all_query(x):
    if "All" in x:
        raise ValueError("Please deselect 'All'!")


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
        "Select Counties", ["All"] + postcodes, default="All"
    )
    if selected_postcodes == ["All"]:
        selected_bers = bers
    else:
        _raise_for_all_query(selected_postcodes)
        selected_bers = bers[bers["countyname"].str.title().isin(selected_postcodes)]
    return selected_bers


if __name__ == "__main__":
    main()

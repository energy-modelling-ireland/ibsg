import re

import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg.fetch import fetch
from ibsg import _LOCAL
from ibsg import _DATA_DIR


def _load_2016_small_area_statistics(url: str) -> pd.DataFrame:
    filepath = fetch(
        url,
        _LOCAL,
        _DATA_DIR,
    )
    # extract only columns related to period of construction
    return pd.read_csv(filepath)


def _repeat_rows_on_column(df, on):
    return df.reindex(df.index.repeat(df[on])).drop(columns=on)


def _melt_statistics_to_individual_buildings(
    sa_stats_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Wrangle the stock to individual building level.

    Before:
        GEOGID              T6_2_PRE19H     ...
        SA2017_017001001    19              ...

    After:
        small_area          period_built
        017001001           PRE19H
        017001001           PRE19H

    Args:
        sa_stats_raw (pd.DataFrame): overview of buildings

    Returns:
        pd.DataFrame: individual buildings
    """
    return (
        sa_stats_raw.loc[:, lambda x: re.match(r"T6_2_.*H", x) or x == "GEOGID"]
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .rename(columns=lambda x: re.findall(f"T6_2_(.*)H", x)[0])
        .reset_index()
        .melt(id_vars="small_area", var_name="period_built", value_name="total")
        .query("period_built != 'T'")
        .pipe(_repeat_rows_on_column, on="total")
    )


@st.cache
def load_census_2016_stock(url: str) -> pd.DataFrame:
    census_sa_stats = _load_2016_small_area_statistics(url)
    individual_buildings = _melt_statistics_to_individual_buildings(census_sa_stats)
    individual_buildings["id"] = clean.get_group_id(
        individual_buildings, columns=["small_area", "period_built"]
    )
    return individual_buildings

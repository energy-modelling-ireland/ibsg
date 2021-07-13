from configparser import ConfigParser
import re
from typing import Any
from typing import Dict
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg.fetch import fetch
from ibsg import _LOCAL
from ibsg import _DATA_DIR


def main(
    bers: pd.DataFrame,
    selections: Dict[str, Any],
    config: ConfigParser = CONFIG,
) -> Optional[pd.DataFrame]:
    if selections["census"]:
        census_building_ages = _load_census_2016_stock(
            url=config["urls"]["small_area_statistics_2016"]
        )
        if selections["replace_not_stated"]:
            with st.spinner("Replacing 'Not Stated' Period Built..."):
                inferred_period_built = _replace_not_stated_period_built(
                    census_building_ages
                )

        with st.spinner("Filling the 2016 census building stock with BERs..."):
            small_area_bers = _add_merge_columns_to_bers(bers)
            stock = _fill_stock_with_small_area_bers(
                stock=census_building_ages,
                inferred_period_built=inferred_period_built,
                bers=small_area_bers,
            )
    else:
        stock = bers
    return stock


@st.cache
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
    columns_to_extract = [
        x for x in sa_stats_raw.columns if re.match(r"T6_2_.*H", x) or x == "GEOGID"
    ]
    return (
        sa_stats_raw.loc[:, columns_to_extract]
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .rename(columns=lambda x: re.findall(f"T6_2_(.*)H", x)[0])
        .reset_index()
        .melt(id_vars="small_area", var_name="period_built", value_name="total")
        .query("period_built != 'T'")
        .pipe(_repeat_rows_on_column, on="total")
    )


def _replace_not_stated_period_built(stock: pd.DataFrame) -> pd.Series:
    return (
        stock[["small_area", "period_built"]]
        .assign(period_built=lambda df: df["period_built"].replace({"NS": np.nan}))
        .groupby("small_area")["period_built"]
        .apply(lambda s: s.fillna(s.mode()[0]))
        .reset_index(drop=True)
    )


def _add_merge_columns_to_census_stock(stock: pd.DataFrame) -> pd.DataFrame:
    stock["id"] = clean.get_group_id(stock, columns=["small_area", "period_built"])
    return stock


def _load_census_2016_stock(url: str) -> pd.DataFrame:
    census_sa_stats = _load_2016_small_area_statistics(url)
    individual_buildings = _melt_statistics_to_individual_buildings(census_sa_stats)
    return _add_merge_columns_to_census_stock(individual_buildings)


def _add_merge_columns_to_bers(bers: pd.DataFrame) -> pd.DataFrame:
    bers["period_built"] = pd.cut(
        bers["year_of_construction"],
        bins=[
            -np.inf,
            1919,
            1945,
            1960,
            1970,
            1980,
            1990,
            2000,
            2011,
            np.inf,
        ],
        labels=[
            "PRE19",
            "19_45",
            "46_60",
            "61_70",
            "71_80",
            "81_90",
            "91_00",
            "01_10",
            "11L",
        ],
    )
    bers["id"] = clean.get_group_id(bers, columns=["small_area", "period_built"])
    return bers


def _fill_stock_with_small_area_bers(
    stock: pd.DataFrame, inferred_period_built: pd.Series, bers: pd.DataFrame
) -> pd.DataFrame:
    before_2016 = (
        stock.copy()
        .assign(
            is_period_built_estimated=lambda df: df["period_built"] == "NS",
            period_built=inferred_period_built,
        )
        .merge(
            bers.query("year_of_construction < 2016"),
            on=["small_area", "period_built", "id"],
            how="left",
        )
    )
    after_2016 = bers.query("year_of_construction >= 2016")
    return pd.concat([before_2016, after_2016])

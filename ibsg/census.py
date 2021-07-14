from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import icontract
import numpy as np
import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg import io
from ibsg import _DATA_DIR


def main(
    bers: pd.DataFrame,
    selections: Dict[str, Any],
    config: ConfigParser = CONFIG,
    data_dir: Path = _DATA_DIR,
) -> Optional[pd.DataFrame]:
    raw_census = _load_census_buildings(
        url=config["urls"]["census_buildings_2016"], data_dir=data_dir
    )
    filtered_census = _extract_rows_in_values(raw_census, selections["countyname"])
    if selections["replace_not_stated"]:
        with st.spinner("Replacing 'Not Stated' Period Built..."):
            census = replace_not_stated_period_built_with_mode(filtered_census)
    else:
        census = filtered_census
    with st.spinner("Filling the 2016 census building stock with BERs..."):
        if selections["ber_granularity"] == "countyname":
            merge_columns = ["countyname", "period_built"]
        elif selections["ber_granularity"] == "small_area":
            merge_columns = ["small_area", "period_built"]
        else:
            raise ValueError(
                "Only countyname or small_area ber_granularity are supported"
            )
        stock = _fill_census_with_bers(
            stock=_add_merge_columns_to_census(census, merge_columns),
            bers=_add_merge_columns_to_bers(bers, merge_columns),
            merge_columns=merge_columns + ["id"],
        )
    return stock


@st.cache
def _load_census_buildings(url: str, data_dir: Path) -> pd.DataFrame:
    return io.load(
        read=pd.read_parquet, url=url, data_dir=data_dir, filesystem_name="s3"
    )


@icontract.ensure(lambda result: len(result) > 0)
def _extract_rows_in_values(df: pd.DataFrame, values: List[str]):
    where_rows_in_values = (
        df["countyname"].astype("string").str.lower().isin(map(str.lower, values))
    )
    return df[where_rows_in_values].copy()


def replace_not_stated_period_built_with_mode(stock: pd.DataFrame) -> pd.Series:
    inferred_stock = stock.copy()
    inferred_stock["is_period_built_estimated"] = inferred_stock["period_built"] == "NS"
    inferred_stock["period_built"] = inferred_stock["period_built"].replace(
        {"NS": np.nan}
    )
    modal_period_built = inferred_stock["period_built"].mode()[0]
    modal_period_built_by_small_area = inferred_stock.groupby("small_area")[
        "period_built"
    ].transform(lambda s: s.mode()[0] if not s.mode().empty else modal_period_built)
    inferred_stock["period_built"] = inferred_stock["period_built"].fillna(
        modal_period_built_by_small_area
    )
    return inferred_stock


def _add_merge_columns_to_census(
    stock: pd.DataFrame, merge_columns: List[str]
) -> pd.DataFrame:
    stock["id"] = clean.get_group_id(stock, columns=merge_columns)
    return stock


def _add_merge_columns_to_bers(
    bers: pd.DataFrame, merge_columns: List[str]
) -> pd.DataFrame:
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
    bers["id"] = clean.get_group_id(bers, columns=merge_columns)
    return bers


def _fill_census_with_bers(
    stock: pd.DataFrame, bers: pd.DataFrame, merge_columns: List[str]
) -> pd.DataFrame:
    before_2016 = stock.merge(
        bers.query("year_of_construction < 2016"),
        on=merge_columns,
        how="left",
    )
    after_2016 = bers.query("year_of_construction >= 2016")
    return pd.concat([before_2016, after_2016])

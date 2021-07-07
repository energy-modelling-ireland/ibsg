from configparser import ConfigParser
from io import BytesIO
from typing import List

from zipfile import ZipFile

import icontract
import numpy as np
import pandas as pd
import streamlit as st

from ibsg import census
from ibsg import clean
from ibsg.fetch import fetch
from ibsg import filter
from ibsg import io
from ibsg import _LOCAL
from ibsg import _DATA_DIR

from ibsg import DEFAULTS
from ibsg import CONFIG


def main(zipped_csv_of_bers: BytesIO, config: ConfigParser = CONFIG) -> pd.DataFrame:
    ## Load
    raw_sa_bers = _load_small_area_bers(zipped_csv_of_bers)
    sa_ids_2016 = _load_2016_small_area_ids(url=config["urls"]["small_area_ids_2016"])
    census_stock = census.load_census_2016_stock(
        url=config["urls"]["small_area_statistics_2016"]
    )

    with st.form("Apply Filters"):
        ## Filter
        sa_bers_in_countyname = filter.filter_by_substrings(
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
        filtered_small_area_bers = _filter_small_area_bers(
            bers=sa_bers_in_countyname,
            small_area_ids=sa_ids_2016,
        )

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    st.markdown("Filling Building Stock as of the 2016 Census with BERs...")
    small_area_bers = _add_merge_columns_to_bers(filtered_small_area_bers)
    census_stock_bers = _fill_stock_with_small_area_bers(
        stock=census_stock, bers=small_area_bers
    )
    return census_stock_bers


@st.cache
@icontract.require(
    lambda zipped_csv_of_bers: len(
        [f for f in ZipFile(zipped_csv_of_bers).namelist() if "csv" in f]
    )
    == 1
)
def _load_small_area_bers(zipped_csv_of_bers: BytesIO) -> pd.DataFrame:
    dtype = DEFAULTS["small_areas"]["dtype"]
    mappings = DEFAULTS["small_areas"]["mappings"]
    zip = ZipFile(zipped_csv_of_bers)
    filename = [f for f in zip.namelist() if "csv" in f][0]
    return io.read(zip.open(filename), dtype=dtype, mappings=mappings)


@st.cache
def _load_2016_small_area_ids(url: str) -> List[str]:
    filepath = fetch(
        url,
        _LOCAL,
        _DATA_DIR,
    )
    return pd.read_csv(
        filepath,
        squeeze=True,
    ).to_list()


def _filter_small_area_bers(
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
            clean.get_rows_equal_to_values,
            filter_name="Is valid small area id",
            selected_filters=selected_filters,
            on_column="small_area",
            values=small_area_ids,
        )
    )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_change = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️ Filtering removed {round(percentage_change, 2)}% of buildings ⚠️")
    return clean_bers


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
    stock: pd.DataFrame, bers: pd.DataFrame
) -> pd.DataFrame:
    before_2016 = stock.merge(
        bers.query("year_of_construction < 2016"),
        on=["small_area", "period_built", "id"],
        how="left",
    )
    after_2016 = bers.query("year_of_construction >= 2016")
    return pd.concat([before_2016, after_2016])

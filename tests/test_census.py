import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
import pytest

from ibsg import census
from ibsg import small_areas


@pytest.fixture
def small_area_statistics(shared_datadir):
    return pd.read_csv(shared_datadir / "SAPS2016_SA2017.csv")


@pytest.fixture
def census_buildings(small_area_statistics):
    stock = census._melt_statistics_to_individual_buildings(small_area_statistics)
    return census._add_merge_columns_to_census_stock(stock)


def test_melt_statistics_to_individual_buildings_matches_totals(
    small_area_statistics, census_buildings
):
    expected_totals = (
        small_area_statistics[["GEOGID", "T6_2_TH"]]
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .squeeze()
        .rename("period_built")
        .sort_index()
    )
    totals = census_buildings.groupby("small_area")["period_built"].count().sort_index()
    assert_series_equal(totals, expected_totals)

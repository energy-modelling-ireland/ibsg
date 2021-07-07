import pandas as pd
from pandas.testing import assert_series_equal

from ibsg import census


def test_melt_statistics_to_individual_buildings_matches_totals(shared_datadir):
    sa_stats = pd.read_csv(shared_datadir / "SAPS2016_SA2017.csv")
    expected_totals = (
        sa_stats[["GEOGID", "T6_2_TH"]]
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .squeeze()
        .rename("period_built")
    )
    output = census._melt_statistics_to_individual_buildings(sa_stats)
    totals = output.groupby("small_area")["period_built"].count()
    assert_series_equal(totals, expected_totals)

from pandas.testing import assert_series_equal

from ibsg import census


def test_load_2016_small_area_statistics_extracts_periods_of_construction(
    datadir, monkeypatch
):
    def _mock_fetch(*args, **kwargs):
        return datadir / "SAPS2016_SA2017.csv"

    periods = [
        "PRE19",
        "19_45",
        "46_60",
        "61_70",
        "71_80",
        "81_90",
        "91_00",
        "01_10",
        "11L",
        "NS",
    ]
    output = census._load_2016_small_area_statistics()
    assert len(output.columns) == 12
    assert any(period in c for period in periods for c in output.columns)


def test_melt_census_to_indiv_building_level_matches_totals(datadir, monkeypatch):
    def _mock_fetch(*args, **kwargs):
        return datadir / "SAPS2016_SA2017.csv"

    sa_stats = census._load_2016_small_area_statistics()
    expected_totals = (
        sa_stats[["GEOGID", "T6_2_TH"]]
        .assign(small_area=lambda df: df["GEOGID"].str[7:])
        .drop(columns="GEOGID")
        .set_index("small_area")
        .squeeze()
        .rename("period_built")
    )
    output = census._melt_census_to_indiv_building_level(sa_stats)
    totals = output.groupby("small_area")["period_built"].count()
    assert_series_equal(totals, expected_totals)

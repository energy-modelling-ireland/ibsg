from typing import List
from zipfile import ZipFile

from icontract import ViolationError
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from ibsg import small_areas


@pytest.mark.parametrize(
    "bers,selected_postcodes,counties,expected_output",
    [
        (
            pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]}),
            ["Dublin 11"],
            ["Dublin", "Galway", "Cork"],
            pd.DataFrame({"countyname": ["DUBLIN 11"]}),
        ),
        (
            pd.DataFrame({"countyname": ["Dublin", "Galway", "Cork"]}),
            ["Dublin", "Galway", "Cork"],
            ["Dublin", "Galway", "Cork"],
            pd.DataFrame(
                {
                    "countyname": ["Dublin", "Galway", "Cork"],
                }
            ),
        ),
    ],
)
def test_filter_by_postcodes(
    bers, selected_postcodes, counties, expected_output, monkeypatch
):
    def _mock_multiselect(*args, **kwargs):
        return selected_postcodes

    monkeypatch.setattr("app.st.multiselect", _mock_multiselect)
    output = small_areas._filter_by_substrings(
        df=bers, column_name="countyname", all_substrings=counties
    )
    assert_frame_equal(output, expected_output)


def test_load_small_area_bers_raises_error_on_empty_file(datadir, monkeypatch):
    with pytest.raises(ViolationError):
        small_areas._load_small_area_bers(datadir / "empty_zip_archive.zip")


@pytest.mark.parametrize(
    "filename",
    [
        "anonymised_small_area_ber_sample.zip",
        "anonymised_small_area_ber_sample.csv.zip",
    ],
)
def test_main(filename, datadir, monkeypatch):
    def _mock_load_small_area_ids(*args, **kwargs) -> List[str]:
        return pd.read_csv(datadir / "small_area_ids_2016.csv", squeeze=True).to_list()

    monkeypatch.setattr(
        "ibsg.small_areas._load_small_area_ids", _mock_load_small_area_ids
    )
    small_areas.main(datadir / filename)

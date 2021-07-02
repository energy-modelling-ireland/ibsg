from typing import List

from numpy import exp
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
import pytest

import app


@pytest.mark.parametrize(
    "selected_counties,expected_output",
    [
        (
            ["All"],
            pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]}),
        ),
        (
            ["Dublin", "Cork"],
            pd.DataFrame({"countyname": ["DUBLIN 11", "CO. CORK"]}, index=[0, 2]),
        ),
    ],
)
def test_filter_by_county(selected_counties, expected_output, monkeypatch):
    def _mock_multiselect(*args, **kwargs):
        return selected_counties

    monkeypatch.setattr("app.st.multiselect", _mock_multiselect)
    bers = pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]})
    counties = ["Dublin", "Galway", "Cork"]
    output = app._filter_by_county(bers=bers, counties=counties)
    assert_frame_equal(output, expected_output)


def test_filter_by_county_raises_error(monkeypatch):
    def _mock_multiselect(*args, **kwargs):
        return ["All", "Dublin"]

    monkeypatch.setattr("app.st.multiselect", _mock_multiselect)
    bers = pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]})
    counties = ["Dublin", "Galway", "Cork"]
    with pytest.raises(ValueError):
        app._filter_by_county(bers=bers, counties=counties)


@pytest.mark.parametrize(
    "selected_postcodes,expected_output",
    [
        (
            ["All"],
            pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]}),
        ),
        (
            ["Dublin 11", "Co. Cork"],
            pd.DataFrame({"countyname": ["DUBLIN 11", "CO. CORK"]}, index=[0, 2]),
        ),
    ],
)
def test_filter_by_postcodes(selected_postcodes, expected_output, monkeypatch):
    def _mock_multiselect(*args, **kwargs):
        return selected_postcodes

    monkeypatch.setattr("app.st.multiselect", _mock_multiselect)
    bers = pd.DataFrame({"countyname": ["DUBLIN 11", "CO. GALWAY", "CO. CORK"]})
    counties = ["Dublin", "Galway", "Cork"]
    output = app._filter_by_county(bers=bers, counties=counties)
    assert_frame_equal(output, expected_output)

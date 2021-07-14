from configparser import ConfigParser
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from ibsg import census
from ibsg import DEFAULTS


def test_replace_not_stated_period_built_with_small_area_mode():
    stock = pd.DataFrame(
        {
            "small_area": [0, 0, 0, 0, 1, 1],
            "period_built": ["NS", "PRE19", "PRE19", "11L", "NS", "11L"],
        }
    )
    expected_output = pd.DataFrame(
        {
            "small_area": [0, 0, 0, 0, 1, 1],
            "period_built": ["PRE19", "PRE19", "PRE19", "11L", "11L", "11L"],
            "is_period_built_estimated": [True, False, False, False, True, False],
        }
    )
    output = census.replace_not_stated_period_built_with_mode(stock=stock)
    assert_frame_equal(output, expected_output)


@pytest.mark.parametrize(
    "replace_not_stated",
    [True, False],
    ids=["replace_not_stated is True", "replace_not_stated is False"],
)
@pytest.mark.parametrize(
    "countyname",
    [DEFAULTS["countyname"], ["CO. DUBLIN"]],
    ids=["All countyname", "CO. DUBLIN"],
)
def test_main_on_countyname_bers(
    countyname_bers: pd.DataFrame,
    config: ConfigParser,
    shared_datadir: Path,
    replace_not_stated: bool,
    countyname: List[str],
):
    selections = {
        "ber_granularity": "countyname",
        "replace_not_stated": replace_not_stated,
        "countyname": countyname,
    }
    census.main(
        bers=countyname_bers,
        selections=selections,
        config=config,
        data_dir=shared_datadir,
    )


@pytest.mark.parametrize(
    "replace_not_stated",
    [True, False],
    ids=["replace_not_stated is True", "replace_not_stated is False"],
)
@pytest.mark.parametrize(
    "countyname",
    [DEFAULTS["countyname"], ["CO. DUBLIN"]],
    ids=["All countyname", "CO. DUBLIN"],
)
def test_main_on_small_area_bers(
    small_area_bers: pd.DataFrame,
    config: ConfigParser,
    shared_datadir: Path,
    replace_not_stated: bool,
    countyname: List[str],
):
    selections = {
        "ber_granularity": "small_area",
        "replace_not_stated": replace_not_stated,
        "countyname": countyname,
    }
    census.main(
        bers=small_area_bers,
        selections=selections,
        config=config,
        data_dir=shared_datadir,
    )

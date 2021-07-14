from configparser import ConfigParser
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from ibsg import census


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


def test_main(
    small_area_bers: pd.DataFrame, config: ConfigParser, shared_datadir: Path
):
    selections = {"census": True, "replace_not_stated": True}
    census.main(
        bers=small_area_bers,
        selections=selections,
        config=config,
        data_dir=shared_datadir,
    )

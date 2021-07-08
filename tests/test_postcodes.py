import pandas as pd

from ibsg import postcodes


def test_main(selections, config):
    postcodes.main(selections=selections, config=config)

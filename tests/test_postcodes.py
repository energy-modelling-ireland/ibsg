import pandas as pd

from ibsg import postcodes


def test_main(config):
    postcodes.main(config)

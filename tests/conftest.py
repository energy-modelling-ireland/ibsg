from configparser import ConfigParser
from typing import List

import pandas as pd
import pytest

from ibsg import small_areas


@pytest.fixture
def config(shared_datadir) -> ConfigParser:
    config = ConfigParser()
    config["urls"] = {}
    config["urls"]["small_area_ids_2016"] = str(
        shared_datadir / "small_area_ids_2016.csv"
    )
    config["urls"]["small_area_statistics_2016"] = str(
        shared_datadir / "SAPS2016_SA2017.csv"
    )
    config["urls"]["postcode_bers"] = str(shared_datadir / "BERPublicsearch.parquet")
    config["settings"] = {}
    config["settings"]["sample_size"] = "1"
    return config


@pytest.fixture
def small_area_ids_2016(shared_datadir) -> List[str]:
    return pd.read_csv(
        shared_datadir / "small_area_ids_2016.csv", squeeze=True
    ).to_list()


@pytest.fixture
def small_area_bers(shared_datadir, config, small_area_ids_2016):
    raw_sa_bers = small_areas._load_small_area_bers(
        shared_datadir / "anonymised_small_area_ber_sample.csv.zip",
    )
    filtered_small_area_bers = small_areas._filter_small_area_bers(
        bers=raw_sa_bers, small_area_ids=small_area_ids_2016
    )
    return small_areas._add_merge_columns_to_bers(filtered_small_area_bers)

from configparser import ConfigParser
from typing import List
from _pytest.mark import param

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
    config["urls"]["census_buildings_2016"] = str(
        shared_datadir / "sample_building_ages_2016.parquet"
    )
    config["filesystems"] = {}
    config["filesystems"]["census_buildings_2016"] = "file"
    config["settings"] = {}
    config["settings"]["sample_size"] = "1"

    config["small_area_ids"] = {}
    config["small_area_ids"]["url"] = str(shared_datadir / "small_area_ids_2016.csv")
    config["small_area_ids"]["filesystem"] = "file"

    return config


@pytest.fixture
def selections():
    return {
        "countyname": [
            "Co. Carlow",
            "Co. Cavan",
            "Co. Clare",
            "Co. Cork",
            "Co. Donegal",
            "Co. Dublin",
            "Co. Galway",
            "Co. Kerry",
            "Co. Kildare",
            "Co. Kilkenny",
            "Co. Laois",
            "Co. Leitrim",
            "Co. Limerick",
            "Co. Longford",
            "Co. Louth",
            "Co. Mayo",
            "Co. Meath",
            "Co. Monaghan",
            "Co. Offaly",
            "Co. Roscommon",
            "Co. Sligo",
            "Co. Tipperary",
            "Co. Waterford",
            "Co. Westmeath",
            "Co. Wexford",
            "Co. Wicklow",
            "Cork City",
            "Dublin 1",
            "Dublin 10",
            "Dublin 11",
            "Dublin 12",
            "Dublin 13",
            "Dublin 14",
            "Dublin 15",
            "Dublin 16",
            "Dublin 17",
            "Dublin 18",
            "Dublin 2",
            "Dublin 20",
            "Dublin 22",
            "Dublin 24",
            "Dublin 3",
            "Dublin 4",
            "Dublin 5",
            "Dublin 6",
            "Dublin 6W",
            "Dublin 7",
            "Dublin 8",
            "Dublin 9",
            "Galway City",
            "Limerick City",
            "Waterford City",
        ]
    }


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
    return small_areas._filter_small_area_bers(
        bers=raw_sa_bers, small_area_ids=small_area_ids_2016
    )


@pytest.fixture
def countyname_bers(shared_datadir):
    return pd.read_parquet(shared_datadir / "BERPublicsearch.parquet")

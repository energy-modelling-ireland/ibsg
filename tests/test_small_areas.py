from configparser import ConfigParser

from icontract import ViolationError
import pytest

from ibsg import small_areas


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
def test_main(filename, datadir, shared_datadir):
    config = ConfigParser()
    config["urls"] = {}
    config["urls"]["small_area_ids_2016"] = str(datadir / "small_area_ids_2016.csv")
    config["urls"]["small_area_statistics_2016"] = str(
        shared_datadir / "SAPS2016_SA2017.csv"
    )
    small_areas.main(datadir / filename, config=config)

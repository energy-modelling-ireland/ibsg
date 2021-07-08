from configparser import ConfigParser

from icontract import ViolationError
import pytest

from ibsg import small_areas


def test_load_small_area_bers_raises_error_on_empty_file(shared_datadir):
    with pytest.raises(ViolationError):
        small_areas._load_small_area_bers(shared_datadir / "empty_zip_archive.zip")


@pytest.mark.parametrize(
    "filename",
    [
        "anonymised_small_area_ber_sample.zip",
        "anonymised_small_area_ber_sample.csv.zip",
    ],
)
def test_main_on_nested_zip_files(filename, shared_datadir, selections, config):
    small_areas.main(shared_datadir / filename, selections=selections, config=config)

from configparser import ConfigParser
from pathlib import Path
import sys

import pytest

parentdir = Path(__name__).parent.parent
sys.path.insert(0, str(parentdir))

import app


def test_main_yields_postcode_bers(monkeypatch, tmp_path: Path, config: ConfigParser):
    def _mock_button(*args, **kwargs):
        return True

    monkeypatch.setattr("app.st.button", _mock_button)
    app.main(data_dir=tmp_path, config=config)
    files = list(tmp_path.iterdir())
    assert len(files) > 0


def test_main_yields_small_area_bers(
    shared_datadir: Path, monkeypatch, tmp_path: Path, config: ConfigParser
):
    def _mock_radio(*args, **kwargs):
        return "small_area"

    def _mock_upload_zip(*args, **kwargs):
        return shared_datadir / "sample_building_ages_2016.parquet"

    monkeypatch.setattr("app.st.radio", _mock_radio)
    monkeypatch.setattr("app.small_areas.main", _mock_upload_zip)
    app.main(data_dir=tmp_path, config=config)
    files = list(tmp_path.iterdir())
    assert len(files) > 0

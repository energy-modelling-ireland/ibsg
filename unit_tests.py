import os
from pathlib import Path
from shutil import copyfile
import shutil

import pandas as pd

import app
from app import _download_bers
from app import _filter_bers
from app import _rename_bers_as_csv
from app import _unzip_bers
from app import main
from globals import get_defaults
from globals import get_dtypes


def _find_file_matching_pattern(dirpath: Path, pattern: str) -> Path:
    matching_files = [f for f in dirpath.glob(pattern)]
    return matching_files[0]


def test_unzip_bers(sample_berpublicsearch_zip: Path, tmp_path: Path) -> None:
    unzipped_filepath = tmp_path / "BERPublicsearch.txt" 
    _unzip_bers(sample_berpublicsearch_zip, tmp_path)
    assert unzipped_filepath.exists()


def test_apply_filters_returns_nonempty_dataframe(
    sample_berpublicsearch_txt: Path, tmp_path: Path
) -> None:
    filters = {
        "GroundFloorArea": {"lb": 0, "ub": 1000},
        "LivingAreaPercent": {"lb": 5, "ub": 90},
        "HSMainSystemEfficiency": {"lb": 19, "ub": 600},
        "WHMainSystemEff": {"lb": 19, "ub": 320},
        "HSEffAdjFactor": {"lb": 0.7},
        "WHEffAdjFactor": {"lb": 0.7},
        "DeclaredLossFactor": {"ub": 20},
        "ThermalBridgingFactor": {"lb": 0, "ub": 0.15},
    }
    output_filepath = tmp_path / "BERPublicsearch.csv.gz"
    dtypes = get_dtypes()

    _filter_bers(sample_berpublicsearch_txt, output_filepath, filters, dtypes)

    output = pd.read_csv(output_filepath, compression="gzip")
    assert len(output) == 98


def test_apply_filters_returns_nonempty_dataframe_on_large_data(
    tmp_path: Path
) -> None:
    filters = {
        "GroundFloorArea": {"lb": 0, "ub": 1000},
        "LivingAreaPercent": {"lb": 5, "ub": 90},
        "HSMainSystemEfficiency": {"lb": 19, "ub": 600},
        "WHMainSystemEff": {"lb": 19, "ub": 320},
        "HSEffAdjFactor": {"lb": 0.7},
        "WHEffAdjFactor": {"lb": 0.7},
        "DeclaredLossFactor": {"ub": 20},
        "ThermalBridgingFactor": {"lb": 0, "ub": 0.15},
    }
    output_filepath = tmp_path / "BERPublicsearch.csv.gz"
    dtypes = get_dtypes()
    here = Path(__file__).parent.resolve()
    data_dir = here / "data"
    unzipped_bers = _find_file_matching_pattern(
        data_dir, "BERPublicsearch-*-*-*"
    )
    if unzipped_bers:
        raw_bers = unzipped_bers / "BERPublicsearch.txt"
        _filter_bers(raw_bers, output_filepath, filters, dtypes)
        assert output_filepath.exists()


def test_download_bers_is_monkeypatched(
    monkeypatch_download_bers: None, tmp_path: Path
) -> None:
    defaults = get_defaults()
    expected_output = tmp_path / "BERPublicsearch.zip"

    _download_bers(defaults["download"], savepath=expected_output)

    assert expected_output.exists()

    # 115686 is the number of bytes corresponding to the test sample of 100 rows 
    assert os.path.getsize(expected_output) == 115686


def test_main(
    tmp_path: Path,
    monkeypatch_download_bers,
    monkeypatch,
) -> None:
    monkeypatch.setattr(app.st, "button", lambda x: True)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    
    main(data_dir=data_dir, download_dir=download_dir)

    expected_output = _find_file_matching_pattern(
        download_dir, "BERPublicsearch-*-*-*.csv.gz"
    )
    # The filtered BERs appear in his downloads folder
    assert expected_output.exists()
    # The filtered BERs are nonempty
    assert len(pd.read_csv(expected_output)) > 0
from io import BytesIO
import json
import os
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import responses

from app import _download_bers
from app import _filter_bers
from app import _rename_bers_as_csv
from app import _unzip_bers
from globals import get_defaults
from globals import get_dtypes


@pytest.fixture
def here() -> str:
    return Path(__name__).parent


@pytest.fixture
def sample_berpublicsearch_txt(here: Path, tmp_path: Path) -> Path:
    output_filepath = tmp_path/ "sample-BERPublicsearch.txt"
    copyfile(here / "sample-BERPublicsearch.txt", output_filepath)
    return output_filepath


@pytest.fixture
def sample_bers(sample_berpublicsearch_txt: Path) -> pd.DataFrame:
    return pd.read_csv(sample_berpublicsearch_txt, sep="\t")


@pytest.fixture
def sample_bers_zipped_filepath(sample_bers: pd.DataFrame, tmp_path: Path) -> BytesIO:
    f = sample_bers.to_csv(index=False, sep="\t")
    filepath = tmp_path / "BERPublicsearch.zip"
    with ZipFile(filepath, "w") as zf:
        zf.writestr("BERPublicsearch.txt", f)
    return filepath


@pytest.fixture
def sample_bers_zipped_filepath_bytes(sample_bers_zipped_filepath: Path) -> BytesIO:
    return ZipFile(sample_bers_zipped_filepath).read("BERPublicsearch.txt")


def test_download_bers_is_mocked(
    tmp_path: Path,
    sample_bers_zipped_filepath_bytes: BytesIO,
    monkeypatch_download_bers: None,
) -> None:
    defaults = get_defaults()
    expected_output = tmp_path / "BERPublicsearch.zip"

    _download_bers(defaults["download"], savepath=expected_output)

    assert expected_output.exists()

    # 115550 is the number of bytes corresponding to the test sample of 100 rows 
    assert os.path.getsize(expected_output) == 115550


def test_unzip_bers(sample_bers_zipped_filepath: Path, tmp_path: Path) -> None:
    unzipped_filepath = tmp_path / "BERPublicsearch.txt" 
    _unzip_bers(sample_bers_zipped_filepath, tmp_path)
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

    output = dd.read_csv(output_filepath, compression="gzip")
    assert len(output) > 0


def test_rename_bers_as_csv(tmp_path: Path) -> None:
    input_file = tmp_path / "BERPublicsearch.txt"
    expected_output_file = tmp_path / "BERPublicsearch.csv"
    with open(input_file, "w") as f:
        f.writelines(["This is a test"])
    _rename_bers_as_csv(input_file)
    assert expected_output_file.exists()


def test_download_bers_is_monkeypatched(
    monkeypatch_download_bers: None, tmp_path: Path
) -> None:
    defaults = get_defaults()
    expected_output = tmp_path / "BERPublicsearch.zip"

    _download_bers(defaults["download"], savepath=expected_output)

    assert expected_output.exists()

    # 115686 is the number of bytes corresponding to the test sample of 100 rows 
    assert os.path.getsize(expected_output) == 115686
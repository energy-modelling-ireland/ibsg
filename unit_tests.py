from io import BytesIO
import json
import os
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
import responses

from app import _download_bers
from app import _filter_bers
from app import _unzip_bers


with open("defaults.json") as f:
    DEFAULTS = json.load(f)



@pytest.fixture
def sample_bers_filepath() -> str:
    return "sample-BERPublicsearch.txt"


@pytest.fixture
def sample_bers(sample_bers_filepath: str) -> pd.DataFrame:
    return pd.read_csv(sample_bers_filepath, sep="\t")


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


@responses.activate
def test_download_bers_is_mocked(
    tmp_path: Path, sample_bers_zipped_filepath_bytes: BytesIO
) -> None:
    responses.add(
        responses.POST,
        DEFAULTS["download"]["url"],
        body=sample_bers_zipped_filepath_bytes,
        content_type="application/x-zip-compressed",
        headers={
            "content-disposition": "attachment; filename=BERPublicSearch.zip"
        },
        status=200,
    )
    expected_output = tmp_path / "BERPublicsearch.zip"

    _download_bers(DEFAULTS["download"], savepath=expected_output)

    assert expected_output.exists()

    # 115550 is the number of bytes corresponding to the test sample of 100 rows 
    assert os.path.getsize(expected_output) == 115550


def test_unzip_bers(sample_bers_zipped_filepath: Path, tmp_path: Path) -> None:
    unzipped_filepath = tmp_path / "BERPublicsearch.txt" 
    _unzip_bers(sample_bers_zipped_filepath, tmp_path)
    assert unzipped_filepath.exists()


def test_apply_filters(sample_bers_filepath: str, tmp_path: Path) -> None:
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

    _filter_bers(sample_bers_filepath, output_filepath, filters)

    assert output_filepath.exists()
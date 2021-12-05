from io import BytesIO
import json
import os
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
import responses

from app import _download_bers


with open("defaults.json") as f:
    DEFAULTS = json.load(f)


@pytest.fixture
def sample_bers(tmp_path: Path) -> BytesIO:
    bers = pd.read_csv("sample-BERPublicsearch.txt", sep="\t")
    f = bers.to_csv(index=False, sep="\t")
    filepath = tmp_path / "BERPublicsearch.zip"
    with ZipFile(filepath, "w") as zf:
        zf.writestr("BERPublicsearch.txt", f)
    return ZipFile(filepath).read("BERPublicsearch.txt")


@responses.activate
def test_download_bers_is_mocked(tmp_path: Path, sample_bers: BytesIO) -> None:
    responses.add(
        responses.POST,
        DEFAULTS["download"]["url"],
        body=sample_bers,
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
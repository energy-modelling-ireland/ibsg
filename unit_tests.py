import json
import os
from pathlib import Path
from time import sleep

import pytest
from requests import Request
import responses

from app import _download_bers


with open("defaults.json") as f:
    DEFAULTS = json.load(f)


@responses.activate
def test_download_bers_is_mocked(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        DEFAULTS["download"]["url"],
        body=None,
        content_type="application/x-zip-compressed",
        headers={
            "content-disposition": "attachment; filename=BERPublicSearch.zip"
        },
        status=200,
    )
    expected_output = tmp_path / "BERPublicsearch.zip"

    _download_bers(DEFAULTS["download"], savepath=expected_output)

    assert expected_output.exists()
    assert os.path.getsize(expected_output) == 0
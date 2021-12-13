from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
from responses import RequestsMock

from globals import get_defaults


@pytest.fixture
def zipped_sample_bers(tmp_path: Path) -> BytesIO:
    here = Path(__file__).parent.resolve()
    sample_bers = here / "sample-BERPublicsearch.txt"
    bers = pd.read_csv(sample_bers, sep="\t")
    content = bers.to_csv(index=False, sep="\t")
    file = BytesIO()
    with ZipFile(file, "w") as zf:
        zf.writestr("BERPublicsearch.txt", content)
    return file.getvalue()


@pytest.fixture
def monkeypatch_download_bers(
    zipped_sample_bers: BytesIO,
    responses: RequestsMock,
) -> None:
    defaults = get_defaults()
    responses.add(
        responses.POST,
        defaults["download"]["url"],
        body=zipped_sample_bers,
        headers=defaults["mock-download-response"]["headers"],
        status=200,
    )
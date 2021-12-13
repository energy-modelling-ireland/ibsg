from io import BytesIO
from pathlib import Path
from shutil import copyfile
from zipfile import ZipFile

import pandas as pd
import pytest
from responses import RequestsMock

from globals import get_defaults


@pytest.fixture
def sample_berpublicsearch_txt(tmp_path: Path) -> Path:
    here = Path(__file__).parent.resolve()
    output_filepath = tmp_path / "sample-BERPublicsearch.txt"
    copyfile(here / "sample-BERPublicsearch.txt", output_filepath)
    return output_filepath


@pytest.fixture
def sample_berpublicsearch_df(sample_berpublicsearch_txt: Path) -> pd.DataFrame:
    return pd.read_csv(sample_berpublicsearch_txt, sep="\t")


@pytest.fixture
def sample_berpublicsearch_zip(
    sample_berpublicsearch_df: pd.DataFrame, tmp_path: Path
) -> BytesIO:
    f = sample_berpublicsearch_df.to_csv(index=False, sep="\t")
    filepath = tmp_path / "BERPublicsearch.zip"
    with ZipFile(filepath, "w") as zf:
        zf.writestr("BERPublicsearch.txt", f)
    return filepath


@pytest.fixture
def sample_berpublicsearch_zip_bytes(tmp_path: Path) -> BytesIO:
    bers = pd.read_csv("sample-BERPublicsearch.txt", sep="\t")
    content = bers.to_csv(index=False, sep="\t")
    file = BytesIO()
    with ZipFile(file, "w") as zf:
        zf.writestr("BERPublicsearch.txt", content)
    return file.getvalue()


@pytest.fixture
def zipped_sample_bers(sample_berpublicsearch_txt: Path) -> BytesIO:
    bers = pd.read_csv(sample_berpublicsearch_txt, sep="\t")
    content = bers.to_csv(index=False, sep="\t")
    file = BytesIO()
    with ZipFile(file, "w") as zf:
        zf.writestr("BERPublicsearch.txt", content)
    return file.getvalue()


@pytest.fixture
def monkeypatch_download_bers(
    sample_berpublicsearch_zip_bytes: BytesIO,
    responses: RequestsMock,
) -> None:
    defaults = get_defaults()
    responses.add(
        responses.POST,
        defaults["download"]["url"],
        body=sample_berpublicsearch_zip_bytes,
        headers=defaults["mock-download-response"]["headers"],
        status=200,
    )
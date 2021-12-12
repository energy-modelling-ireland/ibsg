from io import BytesIO
import json
from pathlib import Path
from time import sleep
from zipfile import ZipFile

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
import responses
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

import app


@pytest.fixture
def browser() -> webdriver.Remote:
    browser = webdriver.Remote(
        command_executor="http://selenium:4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME,
    )
    yield browser
    browser.quit()


@pytest.fixture
def sample_bers(tmp_path: Path) -> BytesIO:
    bers = pd.read_csv("sample-BERPublicsearch.txt", sep="\t")
    f = bers.to_csv(index=False, sep="\t")
    filepath = tmp_path / "BERPublicsearch.zip"
    with ZipFile(filepath, "w") as zf:
        zf.writestr("BERPublicsearch.txt", f)
    return ZipFile(filepath).read("BERPublicsearch.txt")


@responses.activate
def test_user_can_download_default_bers(
    browser: webdriver.Remote,
    sample_bers: BytesIO,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    defaults = app._get_defaults()
    responses.add(
        responses.POST,
        defaults["download"]["url"],
        body=sample_bers,
        content_type="application/x-zip-compressed",
        headers={
            "content-disposition": "attachment; filename=BERPublicSearch.zip"
        },
        status=200,
    )
    monkeypatch.setattr(app, "_get_streamlit_download_dir", lambda: tmp_path)
    monkeypatch.setattr(app, "_get_data_dir", lambda: tmp_path)
    expected_output = tmp_path / "BERPublicsearch.csv.gz"

    browser.set_window_size(1024, 768)

    # Bob opens the website
    browser.get("http://web:8000")

    ## Wait 3s for the selenium browser to load the application
    sleep(3)

    # Scrolls down & is delighted to see filters & a download button!
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Clicks download
    browser.find_element_by_xpath('//button[text()="Download?"]').click()

    # The filtered BERs appear in his downloads folder
    assert expected_output.exists()
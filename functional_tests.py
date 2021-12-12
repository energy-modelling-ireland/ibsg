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
import globals


@pytest.fixture
def browser() -> webdriver.Remote:
    browser = webdriver.Remote(
        command_executor="http://selenium:4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME,
    )
    yield browser
    browser.quit()


@pytest.fixture
def zipped_bers(tmp_path: Path) -> BytesIO:
    bers = pd.read_csv("sample-BERPublicsearch.txt", sep="\t")
    content = bers.to_csv(index=False, sep="\t")
    file = BytesIO()
    with ZipFile(file, "w") as zf:
        zf.writestr("BERPublicsearch.txt", content)
    return file.getvalue()


@responses.activate
def test_user_can_download_default_bers(
    browser: webdriver.Remote,
    zipped_bers: BytesIO,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    defaults = app.get_defaults()
    responses.add(
        responses.POST,
        defaults["download"]["url"],
        body=zipped_bers,
        content_type="application/x-zip-compressed",
        headers={
            "content-disposition": "attachment; filename=BERPublicSearch.zip"
        },
        status=200,
    )
    monkeypatch.setattr(globals, "get_streamlit_download_dir", lambda: tmp_path)
    monkeypatch.setattr(globals, "get_data_dir", lambda: tmp_path)
    expected_output = tmp_path / "BERPublicsearch.csv.gz"
    browser.set_window_size(1024, 768)

    # Bob opens the website
    browser.get("http://web:8000")

    ## Wait 3s for the selenium browser to load the application
    sleep(3)

    # Clicks download
    download_button = browser.find_element_by_xpath('//button[text()="Download?"]')
    download_button.click()

    ## Force browser to scroll down to download button
    browser.execute_script("arguments[0].scrollIntoView();", download_button)

    # The filtered BERs appear in his downloads folder
    assert expected_output.exists()

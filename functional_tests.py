from pathlib import Path
from time import sleep

import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
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


def test_user_can_download_default_bers(
    browser: webdriver.Remote,
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    monkeypatch_download_bers: None,
) -> None:
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


def _find_file_matching_pattern(dirpath: Path, pattern: str) -> Path:
    matching_files = [f for f in dirpath.glob(pattern)]
    return matching_files[0]


def test_main(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    monkeypatch_download_bers: None,
) -> None:
    monkeypatch.setattr(app.st, "button", lambda click: True)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    
    app.main(data_dir=data_dir, download_dir=download_dir)

    expected_output = _find_file_matching_pattern(
        tmp_path / "downloads", "BERPublicsearch-*-*-*.csv.gz"
    )
    # The filtered BERs appear in his downloads folder
    assert expected_output.exists()
    # The filtered BERs are nonempty
    assert len(pd.read_csv(expected_output)) > 0
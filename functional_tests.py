from json import load
from time import sleep

import pytest
import responses
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


with open("defaults.json") as f:
    DEFAULTS = load(f)


@pytest.fixture
def browser() -> webdriver.Remote:
    browser = webdriver.Remote(
        command_executor="http://selenium:4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME,
    )
    yield browser
    browser.quit()


@responses.activate
def test_user_can_download_default_bers(browser: webdriver.Remote) -> None:
    responses.add(
        responses.POST,
        DEFAULTS["download"]["url"],
        content_type="application/x-zip-compressed",
        status=200,
        headers={"content-disposition": "attachment; filename=BERPublicSearch.zip"},
    )

    # Bob opens the website
    browser.get("http://web:8000")

    ## Wait 3s for the selenium browser to load the application
    sleep(3)

    # Is delighted to see filters & a download button!

    # Clicks download
    browser.find_element_by_xpath('//button[text()="Download?"]').click()

    # The filtered BERs appear in his downloads folder
    pytest.fail()
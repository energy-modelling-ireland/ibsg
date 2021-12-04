import pytest
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


@pytest.fixture
def browser() -> webdriver.Remote:
    browser = webdriver.Remote(
        command_executor="http://selenium:4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME,
    )
    yield browser
    browser.quit()


def test_user_can_download_default_bers(browser: webdriver.Remote) -> None:
    # Bob opens the website
    browser.get("http://web:8000")

    # Is delighted to see filters & a download button!
    pytest.fail()

    # Clicks download

    # The filtered BERs appear in his downloads folder
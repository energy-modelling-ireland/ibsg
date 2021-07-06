from ibsg import fetch


def test_fetch_uses_url_when_deployed(datadir):
    url = "i_am_the_data.csv"
    filepath = fetch.fetch(url=url, local=None, data_dir=datadir)
    assert filepath == url


def test_fetch_downloads_when_local(datadir, monkeypatch):
    def _mock_urlretrieve(*args, **kwargs):
        with open(datadir / "i_am_the_data.csv", "w") as f:
            f.write("1,2,3")

    monkeypatch.setattr("ibsg.fetch.urlretrieve", _mock_urlretrieve)
    url = "i_am_the_data.csv"
    filepath = fetch.fetch(url=url, local=True, data_dir=datadir)
    assert filepath.name == "i_am_the_data.csv"
    assert filepath.exists()

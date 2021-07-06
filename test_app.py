import pandas as pd

import app


def test_main_downloads_postcodes(datadir, monkeypatch):
    def _mock_button(*args, **kwargs):
        return True

    def _mock_load_postcode_bers(*args, **kwargs):
        return pd.read_parquet(datadir / "BERPublicsearch.parquet")

    monkeypatch.setattr("app.st.button", _mock_button)
    monkeypatch.setattr("app.postcodes._load_postcode_bers", _mock_load_postcode_bers)
    app.main()

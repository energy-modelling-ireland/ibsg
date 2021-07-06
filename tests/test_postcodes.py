import pandas as pd

from ibsg import postcodes


def test_main(datadir, monkeypatch):
    def _mock_load_postcode_bers(*args, **kwargs):
        return pd.read_parquet(datadir / "BERPublicsearch.parquet")

    monkeypatch.setattr("ibsg.postcodes._load_postcode_bers", _mock_load_postcode_bers)
    postcodes.main()

import pandas as pd

from ibsg import clean


def test_log_percentage_lost(capsys, monkeypatch):
    monkeypatch.setattr("ibsg.clean.st.info", lambda x: print(x))

    @clean.log_percentage_lost
    def _remove_less_than_3(
        df: pd.DataFrame,
        filter_name: str,
        column_name: str,
    ) -> pd.DataFrame:
        return df.query(f"`{column_name}` < 3")

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    filter_name = "Remove greater than 3"
    percentage_removed = 60.0
    _remove_less_than_3(df, filter_name, "a")

    captured = capsys.readouterr()
    assert captured.out == f"{percentage_removed}% removed by '{filter_name}'\n"

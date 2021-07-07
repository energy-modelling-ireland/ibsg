import pandas as pd

from ibsg import archetype


def test_get_aggregation_operations():
    df = pd.DataFrame(
        {
            "cat": pd.Series(["a", "b", "c"], dtype="category"),
            "str": pd.Series(["a", "b", "c"], dtype=str),
            "obj": pd.Series(["a", "b", "c"], dtype="object"),
            "int": pd.Series([1, 2, 3], dtype="int64"),
            "float": pd.Series([1.1, 2.2, 3.3], dtype="float64"),
        }
    )
    expected_output = {
        "cat": pd.Series.mode,
        "str": pd.Series.mode,
        "obj": pd.Series.mode,
        "int": "median",
        "float": "median",
    }

    output = archetype._get_aggregation_operations(df)

    assert output == expected_output

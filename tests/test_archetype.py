import pandas as pd
from pandas.testing import assert_frame_equal

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
        "cat": archetype._get_mode_or_first_occurence,
        "str": archetype._get_mode_or_first_occurence,
        "obj": archetype._get_mode_or_first_occurence,
        "int": "median",
        "float": "median",
    }

    output = archetype._get_aggregation_operations(df)

    assert output == expected_output


def test_create_archetypes():
    stock = pd.DataFrame(
        {
            "dwelling_type": [
                "Detached house",
                "Mid-terrace house",
                "End of terrace house",
                "Detached house",
                "Detached house",
                "End of terrace house",
                "Detached house",
                "Detached house",
                "Ground-floor apartment",
                "Ground-floor apartment",
            ],
            "wall_uvalue": [
                1.83,
                2.09,
                0.60,
                1.77,
                0.28,
                0.37,
                0.30,
                2.00,
                0.50,
                0.94,
            ],
            "main_sh_boiler_fuel": [
                "Heating Oil                   ",
                "Mains Gas                     ",
                "Heating Oil                   ",
                "Heating Oil                   ",
                "Heating Oil                   ",
                "Mains Gas                     ",
                "Heating Oil                   ",
                "Heating Oil                   ",
                "Electricity                   ",
                "Mains Gas                     ",
            ],
        }
    )
    expected_output = pd.DataFrame(
        {
            "dwelling_type": [
                "Detached house",
                "End of terrace house",
                "Ground-floor apartment",
            ],
            "wall_uvalue": [1.77, 0.485, 0.72],
            "main_sh_boiler_fuel": [
                "Heating Oil                   ",
                "Heating Oil                   ",
                "Electricity                   ",
            ],
            "sample_size": [5, 2, 2],
        }
    )
    output = archetype.create_archetypes(
        stock=stock, on_columns=["dwelling_type"], sample_size=1
    )
    assert_frame_equal(output, expected_output)

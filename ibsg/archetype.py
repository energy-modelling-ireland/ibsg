from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import icontract
import numpy as np
import pandas as pd

Operations = Dict[str, Union[str, Callable]]


def _get_mode_or_first_occurence(s: pd.Series) -> str:
    mode = s.mode()
    if len(mode) > 1:
        most_common = mode[0]
    else:
        most_common = mode
    return most_common


def _get_aggregation_operations(df):
    numeric_operations = {c: "median" for c in df.select_dtypes("number").columns}
    categorical_operations = {
        c: _get_mode_or_first_occurence
        for c in set(
            df.select_dtypes("object").columns.tolist()
            + df.select_dtypes("string").columns.tolist()
            + df.select_dtypes("category").columns.tolist()
        )
    }
    return {**numeric_operations, **categorical_operations}


@icontract.ensure(lambda result: len(result) > 0)
def create_archetypes(
    stock: pd.DataFrame,
    archetype_name: str,
    index_columns: List[str],
    exclude_columns: List[str] = [],
    sample_size: int = 30,
) -> pd.DataFrame:
    """Create archetypes on stock of significant sample_size.

    Args:
        stock (pd.DataFrame): Building Stock DataFrame
        archetype_name (str): Name of archetype
        index_columns (List[str]): Column names to archetype on
        exclude_columns (List[str]): Column to ignore during aggregation
        sample_size (int, optional): Sample size above which archetyping is considered.
            Defaults to 30.

    Returns:
        pd.DataFrame: Archetypes DataFrame
    """
    archetype_group_sizes = stock.groupby(index_columns).size().rename("sample_size")
    use_columns = set(stock.columns).difference(set(exclude_columns))
    agg_columns = set(use_columns).difference(set(index_columns))
    aggregation_operations = _get_aggregation_operations(stock[agg_columns])
    return (
        stock.loc[:, use_columns]
        .groupby(index_columns)
        .agg(aggregation_operations)
        .join(archetype_group_sizes)
        .query(f"sample_size > {sample_size}")
        .reset_index()
        .assign(archetype=archetype_name)
    )


def flag_known_buildings(stock: pd.DataFrame, on_column: str) -> pd.DataFrame:
    stock["archetype"] = stock[on_column].notnull().map({True: "none", False: np.nan})
    return stock


def fillna_with_archetypes(
    stock: pd.DataFrame, archetypes: pd.DataFrame, archetype_columns: List[str]
) -> pd.DataFrame:
    return (
        stock.set_index(archetype_columns)
        .combine_first(archetypes.set_index(archetype_columns))
        .reset_index()
    )

from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

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


def create_archetypes(
    stock: pd.DataFrame,
    on_columns: List[str],
    aggregation_operations: Optional[Operations] = None,
    sample_size: int = 30,
) -> pd.DataFrame:
    """Create archetypes on stock of significant sample_size.

    Args:
        stock (pd.DataFrame): Building Stock DataFrame
        on_columns (List[str]): Column names to archetype on
        aggregation_operations (Optional[Operations], optional): Column names mapped to
            aggregation operations (mode, median, mean). Defaults to None.
        sample_size (int, optional): Sample size above which archetyping is considered.
            Defaults to 30.

    Returns:
        pd.DataFrame: Archetypes DataFrame
    """
    archetype_group_sizes = stock.groupby(on_columns).size().rename("sample_size")
    if aggregation_operations:
        use_columns = on_columns + list(aggregation_operations.keys())
    else:
        use_columns = stock.columns
        agg_columns = list(set(use_columns).difference(on_columns))
        aggregation_operations = _get_aggregation_operations(stock[agg_columns])

    return (
        stock.loc[:, use_columns]
        .groupby(on_columns)
        .agg(aggregation_operations)
        .join(archetype_group_sizes)
        .query(f"sample_size > {sample_size}")
        .reset_index()
    )

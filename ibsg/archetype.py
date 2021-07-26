from typing import List

import icontract
import numpy as np
import pandas as pd


def _get_mode_or_first_occurence(srs: pd.Series) -> str:
    m = pd.Series.mode(srs)
    return m.values[0] if not m.empty else np.nan


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
def _create_archetypes(
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
    sample_size_name = f"sample_size_{archetype_name}"
    archetype_group_sizes = stock.groupby(index_columns).size().rename(sample_size_name)
    use_columns = set(stock.columns).difference(set(exclude_columns))
    agg_columns = set(use_columns).difference(set(index_columns))
    aggregation_operations = _get_aggregation_operations(stock[agg_columns])
    return (
        stock.loc[:, use_columns]
        .groupby(index_columns)
        .agg(aggregation_operations)
        .join(archetype_group_sizes)
        .query(f"`{sample_size_name}` > {sample_size}")
        .reset_index()
        .assign(archetype=archetype_name)
    )


def fillna_with_archetypes(
    buildings: pd.DataFrame, archetype_columns: List[str], sample_size: int
) -> pd.DataFrame:
    for columns in archetype_columns:
        archetypes = _create_archetypes(
            stock=buildings,
            archetype_name=str(columns),
            index_columns=columns,
            exclude_columns=["id"],
            sample_size=sample_size,
        )
        buildings = (
            buildings.set_index(columns)
            .combine_first(archetypes.set_index(columns))
            .reset_index()
        )
    return buildings

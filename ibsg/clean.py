from typing import List
from typing import Union

import dask.dataframe as dd
import pandas as pd
import icontract


@icontract.ensure(lambda result: len(result) != 0)
def get_rows_meeting_condition(
    ber: Union[pd.DataFrame, dd.DataFrame],
    filter_name: str,
    selected_filters: List[str],
    condition: str,
) -> pd.DataFrame:
    if filter_name in selected_filters:
        filtered_ber = ber.query(condition)
    else:
        filtered_ber = ber
    return filtered_ber


@icontract.ensure(lambda result: len(result) != 0)
def get_rows_equal_to_values(
    ber: Union[pd.DataFrame, dd.DataFrame],
    filter_name: str,
    selected_filters: List[str],
    on_column: str,
    values: List[str],
) -> pd.DataFrame:
    if filter_name in selected_filters:
        # values & column must be of same type or query will be empty!
        filtered_ber = ber[ber[on_column].astype("string").isin(values)]
    else:
        filtered_ber = ber
    return filtered_ber

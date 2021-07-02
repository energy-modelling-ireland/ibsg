from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd
from pandas.api.types import is_string_dtype
import icontract
from typeguard import typechecked

from ibsg import DEFAULTS


def standardise_ber_private_column_names(
    ber: pd.DataFrame,
    mappings: Optional[Dict[str, str]] = DEFAULTS["mappings"]["small_area_bers"],
):
    return ber.rename(columns=mappings)


@typechecked
@icontract.ensure(lambda result: len(result) != 0)
def get_rows_meeting_condition(
    ber: pd.DataFrame,
    filter_name: str,
    selected_filters: List[str],
    condition: str,
) -> pd.DataFrame:
    if filter_name in selected_filters:
        filtered_ber = ber.query(condition)
    else:
        filtered_ber = ber
    return filtered_ber


@typechecked
@icontract.ensure(lambda result: len(result) != 0)
def get_rows_equal_to_values(
    ber: pd.DataFrame,
    filter_name: str,
    selected_filters: List[str],
    on_column: str,
    values: List[str],
) -> pd.DataFrame:
    if filter_name in selected_filters:
        # values & column must be of same type or query will be empty!
        ber.loc[:, on_column] = ber[on_column].astype("string")
        filtered_ber = ber[ber[on_column].isin(values)]
    else:
        filtered_ber = ber
    return filtered_ber

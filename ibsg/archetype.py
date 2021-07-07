from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pandas as pd

Operations = Dict[str, Union[str, Callable]]


def _get_aggregation_operations(df):
    numeric_operations = {c: "median" for c in df.select_dtypes("number").columns}
    categorical_operations = {
        c: pd.Series.mode
        for c in set(
            df.select_dtypes("object").columns.tolist()
            + df.select_dtypes("string").columns.tolist()
            + df.select_dtypes("category").columns.tolist()
        )
    }
    return {**numeric_operations, **categorical_operations}

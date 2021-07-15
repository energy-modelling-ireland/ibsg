from typing import List

import pandas as pd
import icontract
import streamlit as st


def log_percentage_lost(f):
    def inner(df: pd.DataFrame, filter_name: str, *args, **kwargs):
        result = f(df, filter_name, *args, **kwargs)
        len_before = len(df)
        len_after = len(result)
        percentage_lost = 100 * (len_before - len_after) / len_before
        st.info(f"{round(percentage_lost, 2)}% removed by '{filter_name}'")
        return f(df, filter_name, *args, **kwargs)

    return inner


@log_percentage_lost
@icontract.ensure(lambda result: len(result) != 0)
def get_rows_meeting_condition(
    df: pd.DataFrame,
    filter_name: str,
    selected_filters: List[str],
    condition: str,
) -> pd.DataFrame:
    if filter_name in selected_filters:
        filtered_df = df.query(condition)
    else:
        filtered_df = df
    return filtered_df


@log_percentage_lost
@icontract.ensure(lambda result: len(result) != 0)
def get_rows_equal_to_values(
    df: pd.DataFrame,
    filter_name: str,
    selected_filters: List[str],
    on_column: str,
    values: List[str],
) -> pd.DataFrame:
    if filter_name in selected_filters:
        # values & column must be of same type or query will be empty!
        filtered_df = df[df[on_column].astype("string").isin(values)]
    else:
        filtered_df = df
    return filtered_df


def get_group_id(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    return df.groupby(columns).cumcount().apply(lambda x: x + 1)

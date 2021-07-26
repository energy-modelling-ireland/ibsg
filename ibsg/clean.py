from logging import warning
from typing import List
import warnings

import pandas as pd
import icontract
import streamlit as st
from streamlit import report_thread


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
        try:
            filtered_df = df.query(condition)
        except:
            _warn(f"Column called in {condition} does not exist")
            filtered_df = df
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
        try:
            # values & column must be of same type or query will be empty!
            filtered_df = df[df[on_column].astype("string").isin(values)]
        except:
            _warn(f"Column '{on_column}'' does not exist")
            filtered_df = df
    else:
        filtered_df = df
    return filtered_df


def get_rows_containing_substrings(
    df: pd.DataFrame,
    column_name: str,
    selected_substrings: List[str],
    all_substrings: List[str],
) -> pd.DataFrame:
    if selected_substrings == all_substrings:
        selected_df = df
    else:
        substrings_to_search = "|".join(map(str.lower, selected_substrings))
        selected_df = df[
            df[column_name].str.lower().str.contains(substrings_to_search, regex=True)
        ]
    return selected_df


def get_group_id(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    return df.groupby(columns).cumcount().apply(lambda x: x + 1)


def _warn(body: str) -> None:
    is_streamlit_thread = report_thread.get_report_ctx()
    if is_streamlit_thread:
        return st.warning(body)
    else:
        warnings.warn(body)

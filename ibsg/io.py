import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict
from typing import Union

import dask.dataframe as dd
import pandas as pd
import streamlit as st


def read(
    file: Union[BytesIO, str, Path],
    dtype: Dict[str, str],
    mappings: Dict[str, str],
    sep: str = ",",
    engine: str = "pandas",
) -> Union[pd.DataFrame, dd.DataFrame]:
    """Read BER Small Areas or Postcodes to a DataFrame.

    Args:
        file (Union[BytesIO, str, Path]): A filepath or file-like object of data
        dtype (Dict[str, str]): Data types for each column
        mappings (Dict[str, str]): Mappings to standardise each column name
        sep (str, optional): [description]. Csv separator. Defaults to ",".
        engine (str, optional): [description]. Engine to read csv. Defaults to "pandas".

    Returns:
        Union[pd.DataFrame, dd.DataFrame]: DataFrame of the data
    """
    if engine == "pandas":
        df = pd.read_csv(
            file,
            sep=sep,
            dtype=dtype,
            encoding="latin-1",
            low_memory=False,
            usecols=list(dtype.keys()),
        ).rename(columns=mappings)
    elif engine == "dask":
        df = dd.read_csv(
            file,
            sep=sep,
            dtype=dtype,
            encoding="latin-1",
            low_memory=False,
            usecols=list(dtype.keys()),
        ).rename(columns=mappings)
    else:
        raise ValueError(f"Only 'pandas' and 'dask' are supported engines")
    return df


def _create_csv_download_link(
    df: Union[pd.DataFrame, dd.DataFrame], filename: str, engine: str
) -> None:
    # workaround from streamlit/streamlit#400
    STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
    DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
    if not DOWNLOADS_PATH.is_dir():
        DOWNLOADS_PATH.mkdir()
    filepath = (DOWNLOADS_PATH / filename).with_suffix(".csv.gz")

    if engine == "pandas":
        df.to_csv(filepath, index=False, compression="gzip")
    elif engine == "dask":
        df.to_csv(filepath, index=False, compression="gzip", single_file=True)
    else:
        raise ValueError(f"Only 'pandas' and 'dask' are supported engines")

    st.markdown(f"[{filepath.name}](downloads/{filepath.name})")


def download_as_csv(
    df: Union[pd.DataFrame, dd.DataFrame], category: str, engine: str = "pandas"
) -> None:
    save_to_csv_selected = st.button("Save to csv.gz?")
    if save_to_csv_selected:
        _create_csv_download_link(
            df=df, filename=f"{category}_bers_{datetime.date.today()}", engine=engine
        )

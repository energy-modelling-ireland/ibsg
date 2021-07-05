from pathlib import Path
from zipfile import ZipFile

from ber_api import request_public_ber_db
import icontract
from icontract import ViolationError
import pandas as pd
from stqdm import stqdm
import streamlit as st

from ibsg import filter
from ibsg import io

import streamlit as st


def main(email_address: str) -> pd.DataFrame:
    ## Download
    filepath = Path.cwd() / "BERPublicsearch.zip"
    if not filepath.exists():
        st.write(
            f"Accessing {filepath.name}"
            " from https://ndber.seai.ie/BERResearchTool/Register/Register.aspx"
        )
        request_public_ber_db(email_address=email_address, tqdm_bar=stqdm)

    postcode_bers_raw = _load_postcode_bers(filepath)

    with st.form("Apply Filters"):
        ## Filter
        postcode_bers_in_countyname = filter.filter_by_substrings(
            postcode_bers_raw,
            column_name="countyname",
            all_substrings=[
                "Co. Carlow",
                "Co. Cavan",
                "Co. Clare",
                "Co. Cork",
                "Co. Donegal",
                "Co. Dublin",
                "Co. Galway",
                "Co. Kerry",
                "Co. Kildare",
                "Co. Kilkenny",
                "Co. Laois",
                "Co. Leitrim",
                "Co. Limerick",
                "Co. Longford",
                "Co. Louth",
                "Co. Mayo",
                "Co. Meath",
                "Co. Monaghan",
                "Co. Offaly",
                "Co. Roscommon",
                "Co. Sligo",
                "Co. Tipperary",
                "Co. Waterford",
                "Co. Westmeath",
                "Co. Wexford",
                "Co. Wicklow",
                "Cork City",
                "Dublin 1",
                "Dublin 10",
                "Dublin 11",
                "Dublin 12",
                "Dublin 13",
                "Dublin 14",
                "Dublin 15",
                "Dublin 16",
                "Dublin 17",
                "Dublin 18",
                "Dublin 2",
                "Dublin 20",
                "Dublin 22",
                "Dublin 24",
                "Dublin 3",
                "Dublin 4",
                "Dublin 5",
                "Dublin 6",
                "Dublin 6W",
                "Dublin 7",
                "Dublin 8",
                "Dublin 9",
                "Galway City",
                "Limerick City",
                "Waterford City",
            ],
        )
        clean_postcode_bers = _clean_postcode_bers(postcode_bers_in_countyname)

        ## Submit
        st.form_submit_button(label="Re-apply Filters")


@st.cache
@icontract.require(
    lambda filepath: len(
        [f for f in ZipFile(filepath).namelist() if "BERPublicsearch.txt" in f]
    )
    == 1,
    error=lambda filepath: ViolationError(
        f"BERPublicsearch.txt not found in {filepath}"
    ),
)
def _load_postcode_bers(filepath: Path) -> pd.DataFrame:
    zip = ZipFile(filepath)
    filename = [f for f in zip.namelist() if "BERPublicsearch.txt" in f][0]
    return io.read(zip.open(filename), category="postcodes", sep="\t")


def _clean_postcode_bers(bers: pd.DataFrame) -> pd.DataFrame:
    return bers

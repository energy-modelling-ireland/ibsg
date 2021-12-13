import csv
from datetime import datetime
from pathlib import Path
from shutil import copyfile
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from zipfile import ZipFile

import dask.dataframe as dd
import pandas as pd
import requests
from stqdm import stqdm
import streamlit as st

from globals import get_data_dir
from globals import get_streamlit_download_dir
from globals import get_defaults
from globals import get_dtypes


def _select_ber_filters() -> Tuple[List[str], Dict[str, Dict[str, int]]]:
    filter_names = [
        "Is not provisional",
        "lb < GroundFloorArea < ub",
        "lb < LivingAreaPercent < ub",
        "lb < HSMainSystemEfficiency < ub",
        "lb < WHMainSystemEff < ub",
        "HSEffAdjFactor > lb",
        "WHEffAdjFactor > lb",
        "DeclaredLossFactor < ub",
        "lb < ThermalBridgingFactor < ub",
    ]
    selected_filters: List[str] = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
        help="lb = Lower Bound, ub = Upper Bound",
    )

    with st.expander("Change BER Filter Bounds?"):
        c1, c2 = st.columns(2)
        return selected_filters, {
            "GroundFloorArea": {
                "lb": c1.number_input("Lower Bound: GroundFloorArea", value=0),
                "ub": c2.number_input("Upper Bound: GroundFloorArea", value=1000),
            },
            "LivingAreaPercent": {
                "lb": c1.number_input("Lower Bound: LivingAreaPercent", value=5),
                "ub": c2.number_input("Lower Bound: LivingAreaPercent", value=90),
            },
            "HSMainSystemEfficiency": {
                "lb": c1.number_input("Lower Bound: HSMainSystemEfficiency", value=19),
                "ub": c2.number_input("Lower Bound: HSMainSystemEfficiency", value=600),
            },
            "WHMainSystemEff": {
                "lb": c1.number_input("Lower Bound: WHMainSystemEff", value=19),
                "ub": c2.number_input("Lower Bound: WHMainSystemEff", value=320),
            },
            "HSEffAdjFactor": {
                "lb": st.number_input(
                    "Lower Bound: HSEffAdjFactor",
                    value=0.7,
                ),
            },
            "WHEffAdjFactor": {
                "lb": st.number_input(
                    "Lower Bound: WHEffAdjFactor",
                    value=0.7,
                ),
            },
            "DeclaredLossFactor": {
                "ub": st.number_input(
                    "Upper Bound: DeclaredLossFactor",
                    value=20,
                ),
            },
            "ThermalBridgingFactor": {
                "lb": c1.number_input("Lower Bound: ThermalBridgingFactor", value=0),
                "ub": c2.number_input("Lower Bound: ThermalBridgingFactor", value=0.15),
            },
        }


def _download_bers(form: Dict[str, str], savepath: Path) -> None:
    response = requests.post(
        url=form["url"],
        headers=form["headers"],
        cookies=form["cookies"],
        data=form["data"],
        stream=True,
    )

    # raise an HTTPError if the HTTP request returned an unsuccessful status code.
    response.raise_for_status()

    with stqdm.wrapattr(
        open(str(savepath), "wb"),
        "write",
        miniters=1,
        desc=str(savepath),
        total=int(response.headers.get("content-length", 0)),
    ) as fout:
        for chunk in response.iter_content(chunk_size=4096):
            fout.write(chunk)


def _unzip_bers(input_filepath: Path, output_dirpath: Path) -> None:
    ZipFile(input_filepath).extractall(output_dirpath)


def _rename_bers_as_csv(input_filepath: Path) -> None:
    csv_filename = input_filepath.with_suffix(".csv")
    input_filepath.rename(csv_filename)


def _filter_bers(
    input_filepath: Path,
    output_filepath: Path,
    filters: Dict[str, Any],
    dtypes: Dict[str, str],
) -> None:
    bers = dd.read_csv(
        input_filepath,
        sep="\t",
        encoding="latin-1",
        quoting=csv.QUOTE_NONE,
        dtype=dtypes,
    )
    clean_bers = (
        bers.query("TypeofRating != 'Provisional    '")
        .query(
            f"GroundFloorArea > {filters['GroundFloorArea']['lb']}"
            f" and GroundFloorArea < {filters['GroundFloorArea']['ub']}",
        )
        .query(
            f"LivingAreaPercent > {filters['LivingAreaPercent']['lb']}"
            f" or LivingAreaPercent < {filters['LivingAreaPercent']['ub']}",
        )
        .query(
            f"HSMainSystemEfficiency > {filters['HSMainSystemEfficiency']['lb']}"
            f" or HSMainSystemEfficiency < {filters['HSMainSystemEfficiency']['ub']}",
        )
        .query(
            f"WHMainSystemEff > {filters['WHMainSystemEff']['lb']}"
            f" or WHMainSystemEff < {filters['WHMainSystemEff']['ub']}",
        )
        .query(
            f"HSEffAdjFactor > {filters['HSEffAdjFactor']['lb']}",
        )
        .query(
            f"WHEffAdjFactor > {filters['WHEffAdjFactor']['lb']}",
        )
        .query(
            f"DeclaredLossFactor < {filters['DeclaredLossFactor']['ub']}",
        )
        .query(
            f"ThermalBridgingFactor > {filters['ThermalBridgingFactor']['lb']}"
            f" or ThermalBridgingFactor <= {filters['ThermalBridgingFactor']['ub']}",
        )
    )
    clean_bers.to_csv(
        output_filepath, header=True, single_file=True, compression="gzip", index=False
    )


def _generate_bers(
    data_dir: Path,
    download_dir: Path,
    filename: str,
    selections: Dict[str, Any],
    defaults: Dict[str, Any],
    dtypes: Dict[str, str],
) -> None:
    with st.spinner("Downloading `BERPublicsearch.zip` from `ndber.seai.ie` ..."):
        _download_bers(defaults["download"], data_dir / "BERPublicsearch.zip")
    
    with st.spinner("Unzipping `BERPublicsearch.zip` ..."):
        _unzip_bers(data_dir / "BERPublicsearch.zip", data_dir / "BERPublicsearch")
    
    with st.spinner("Filtering BERs ..."):
        _filter_bers(
            data_dir / "BERPublicsearch" / "BERPublicsearch.txt",
            data_dir / filename,
            filters=selections["bounds"],
            dtypes=dtypes,
        )
    
    with st.spinner("Saving BERs ..."):
        copyfile(data_dir / filename, download_dir / filename)


def main(
    data_dir: Path = get_data_dir(),
    download_dir: Path = get_streamlit_download_dir(),
    defaults: Dict[str, Any] = get_defaults(),
    dtypes: Dict[str, str] = get_dtypes(),
):
    st.markdown(
        """
        # Irish Building Stock Generator
        
        Download a standardised version of the Irish Building Energy Ratings.
        
        > If you have any problems or questions please raise an issue on our [Github](https://github.com/energy-modelling-ireland/ibsg) 
        """
    )

    selections = {}
    selections["countyname"] = st.multiselect(
        f"Select countyname",
        options=defaults["countyname"],
        default=defaults["countyname"],
        help="""Extract only buildings in certain selected 'countyname' or postcodes
        such as 'Co. Dublin'""",
    )

    selections["filters"], selections["bounds"] = _select_ber_filters()
    selections["download_filetype"] = st.selectbox(
        "Download format?",
        options=[".csv.gz", ".parquet"],
        help="You might need to install 7zip to unzip '.csv.gz' (see hints)",
    )
    if st.button("Download?"):
        today = datetime.today()
        filename = f"BERPublicsearch-{today:%d-%m-%Y}.csv.gz"
        _generate_bers(
            data_dir=data_dir,
            download_dir=download_dir,
            filename=filename,
            selections=selections,
            defaults=defaults,
            dtypes=dtypes,
        )
        st.info(f"[{filename}](downloads/{filename})")


if __name__ == "__main__":
    main()

from configparser import ConfigParser
from datetime import datetime
from json import load
from pathlib import Path
from shutil import copyfile
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from zipfile import ZipFile

import pandas as pd
import requests
from stqdm import stqdm
import streamlit as st


def _get_data_dir() -> Path:
    data_dir = Path(__file__).parent / "data"
    if not data_dir.exists():
        data_dir.mkdir()


def _get_streamlit_download_dir() -> Path:
    # workaround from streamlit/streamlit#400
    download_dir = Path(st.__path__[0]) / "static" / "downloads"
    if not download_dir.exists():
        download_dir.mkdir()


def _get_defaults() -> Dict[str, Any]:
    with open("defaults.json") as f:
        return load(f)


def _get_config() -> ConfigParser:
    config = ConfigParser()
    config.read("config.ini")
    return config


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
                "lb": c1.number_input(
                    "Lower Bound: HSMainSystemEfficiency", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: HSMainSystemEfficiency", value=600
                ),
            },
            "WHMainSystemEff": {
                "lb": c1.number_input(
                    "Lower Bound: WHMainSystemEff", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: WHMainSystemEff", value=320
                ),
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
                "ub": c2.number_input(
                    "Lower Bound: ThermalBridgingFactor", value=0.15
                ),
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
        total=int(response.headers.get('content-length', 0))
    ) as fout:
        for chunk in response.iter_content(chunk_size=4096):
            fout.write(chunk)


def _unzip_bers(input_filepath: Path, output_filepath: Path) -> None:
    ZipFile(input_filepath).extractall(output_filepath)


def _filter_bers(
    input_filepath: Path, output_filepath: Path, filters: Dict[str, Any]
) -> None:
    bers = pd.read_csv(input_filepath, sep="\t")

    conditions = [
        "TypeofRating != 'Provisional    '",
        f"GroundFloorArea > {filters['GroundFloorArea']['lb']}"
        f"and GroundFloorArea < {filters['GroundFloorArea']['lb']}",
        f"LivingAreaPercent > {filters['LivingAreaPercent']['lb']}"
        f"or LivingAreaPercent < {filters['LivingAreaPercent']['ub']}",
        f"HSMainSystemEfficiency > {filters['HSMainSystemEfficiency']['lb']}"
        f"or HSMainSystemEfficiency < {filters['HSMainSystemEfficiency']['ub']}",
        f"WHMainSystemEff > {filters['WHMainSystemEff']['lb']}"
        f"or WHMainSystemEff < {filters['WHMainSystemEff']['ub']}",
        f"HSEffAdjFactor > {filters['HSEffAdjFactor']['lb']}",
        f"WHEffAdjFactor > {filters['WHEffAdjFactor']['lb']}",
        f"DeclaredLossFactor < {filters['DeclaredLossFactor']['ub']}",
        f"ThermalBridgingFactor > {filters['ThermalBridgingFactor']['lb']}"
        f"or ThermalBridgingFactor <= {filters['ThermalBridgingFactor']['ub']}",
    ]
    query_str = " and ".join(["(" + c + ")" for c in conditions])
    buildings_meeting_conditions = bers.query(query_str)

    buildings_meeting_conditions.to_csv(output_filepath)


def _generate_bers(
    data_dir: Path,
    download_dir: Path,
    filename: str,
    selections: Dict[str, Any],
    defaults: Dict[str, Any],
) -> None:
    _download_bers(defaults["download"], data_dir / "BERPublicsearch.zip")
    _unzip_bers(data_dir / "BERPublicsearch.zip", data_dir)
    _filter_bers(
        data_dir / "BERPublicsearch" / "BERPublicsearch.txt",
        data_dir / filename,
        filters=selections["bounds"]
    )
    breakpoint()
    copyfile(
        data_dir / filename, download_dir / filename
    )


def main(
    data_dir: Path = _get_data_dir(),
    download_dir: Path = _get_streamlit_download_dir(),
    config: ConfigParser = _get_config(),
    defaults: Dict[str, Any] = _get_defaults(),
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
        )
        st.markdown(f"[{filename}](downloads/{filename})")



if __name__ == "__main__":
    main()

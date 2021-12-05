from configparser import ConfigParser
from datetime import datetime
from json import load
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from zipfile import ZipFile

import requests
from stqdm import stqdm
import streamlit as st


DATA_DIR = Path(__file__).parent / "data"
if not DATA_DIR.exists():
    DATA_DIR.mkdir()

# workaround from streamlit/streamlit#400
DOWNLOAD_DIR = Path(st.__path__[0]) / "static" / "downloads"
if not DOWNLOAD_DIR.exists():
    DOWNLOAD_DIR.mkdir()

with open("defaults.json") as f:
    DEFAULTS = load(f)

CONFIG = ConfigParser()
CONFIG.read("config.ini")


def _select_ber_filters() -> Tuple[List[str], Dict[str, Dict[str, int]]]:
    filter_names = [
        "Is not provisional",
        "lb < ground_floor_area < ub",
        "lb < living_area_percent < ub",
        "lb < main_sh_boiler_efficiency < ub",
        "lb < main_hw_boiler_efficiency < ub",
        "main_sh_boiler_efficiency_adjustment_factor > lb",
        "main_hw_boiler_efficiency_adjustment_factor > lb",
        "declared_loss_factor < ub",
        "lb < thermal_bridging_factor < ub",
        "Is valid small area id",
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
            "ground_floor_area": {
                "lb": c1.number_input("Lower Bound: ground_floor_area", value=0),
                "ub": c2.number_input("Upper Bound: ground_floor_area", value=1000),
            },
            "living_area_percent": {
                "lb": c1.number_input("Lower Bound: living_area_percent", value=5),
                "ub": c2.number_input("Lower Bound: living_area_percent", value=90),
            },
            "main_sh_boiler_efficiency": {
                "lb": c1.number_input(
                    "Lower Bound: main_sh_boiler_efficiency", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: main_sh_boiler_efficiency", value=600
                ),
            },
            "main_hw_boiler_efficiency": {
                "lb": c1.number_input(
                    "Lower Bound: main_hw_boiler_efficiency", value=19
                ),
                "ub": c2.number_input(
                    "Lower Bound: main_hw_boiler_efficiency", value=320
                ),
            },
            "main_sh_boiler_efficiency_adjustment_factor": {
                "lb": st.number_input(
                    "Lower Bound: main_sh_boiler_efficiency_adjustment_factor",
                    value=0.7,
                ),
            },
            "main_hw_boiler_efficiency_adjustment_factor": {
                "lb": st.number_input(
                    "Lower Bound: main_hw_boiler_efficiency_adjustment_factor",
                    value=0.7,
                ),
            },
            "declared_loss_factor": {
                "ub": st.number_input(
                    "Upper Bound: declared_loss_factor",
                    value=20,
                ),
            },
            "thermal_bridging_factor": {
                "lb": c1.number_input("Lower Bound: thermal_bridging_factor", value=0),
                "ub": c2.number_input(
                    "Lower Bound: thermal_bridging_factor", value=0.15
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


def _generate_bers(
    data_dir: Path,
    filename: str,
    selections: Dict[str, Any],
    config: ConfigParser,
    defaults: Dict[str, Any],
) -> None:
    _download_bers(defaults["download"], data_dir / "BERPublicsearch.zip")
    _unzip_bers(data_dir / "BERPublicsearch.zip", data_dir / "BERPublicsearch")


def main(
    data_dir: Path = DATA_DIR,
    download_dir: Path = DOWNLOAD_DIR,
    config: ConfigParser = CONFIG,
    defaults: Dict[str, Any] = DEFAULTS,
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
            filename=filename,
            selections=selections,
            config=config,
            defaults=defaults,
        )
        st.markdown(f"[{filename}](downloads/{filename})")



if __name__ == "__main__":
    main()

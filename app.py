from collections import defaultdict
from configparser import ConfigParser
import datetime
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import streamlit as st

from ibsg import CONFIG
from ibsg import _DATA_DIR
from ibsg import DEFAULTS
from ibsg.main import generate_building_stock

# workaround from streamlit/streamlit#400
STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
if not DOWNLOADS_PATH.is_dir():
    DOWNLOADS_PATH.mkdir()


def main(
    data_dir: Path = _DATA_DIR,
    config: ConfigParser = CONFIG,
    defaults: Dict[str, Any] = DEFAULTS,
):
    st.markdown(
        """
        # 🏠 Irish Building Stock Generator 🏠
        
        Generate a standardised building stock at postcode **or** small area level.

        - Select `Fetch Postcode BERs` (i.e.  [`BER Public`](https://ndber.seai.ie/BERResearchTool/Register/Register.aspx))
            
        - **Or** upload a `zip` file containing a `csv` of Small Area BERs (`closed-access`)

        ---

        If you have any problems or questions:

        - Chat with us on [Gitter](https://gitter.im/energy-modelling-ireland/ibsg)
        - Or raise an issue on our [Github](https://github.com/energy-modelling-ireland/ibsg) 
        """
    )
    with st.beta_expander("Hints"):
        st.markdown(
            f"""
        - You might need to install [7zip](https://www.7-zip.org/) to:
            - Read `gz` files
            - Compress your closed-access small area BER dataset to a `zip` file to
            under 200MB
        - `ibsg` won't be able to read your zipped Small Area BERs `csv` file if the
        column names don't match:

        `{list(defaults['small_areas']['mappings'].keys())}`
        """
        )

    selections = {}
    selections["ber_granularity"] = st.radio(
        "BER Granularity",
        options=["countyname", "small_area"],
        help="""Granularity is the extent to which a system is broken down into small
        parts, either the system itself or its description or observation. It is the
        'extent to which a larger entity is subdivided. For example, a yard broken into
        inches has finer granularity than a yard broken into feet.'""",
    )
    if selections["ber_granularity"] == "small_area":
        selections["small_area_bers"] = st.file_uploader(
            "Upload Small Area BERs",
            type="zip",
        )
    selections["countyname"] = st.multiselect(
        f"Select countyname",
        options=defaults["countyname"],
        default=defaults["countyname"],
        help="""Extract only buildings in certain selected 'countyname' or postcodes
        such as 'Co. Dublin'""",
    )
    selections["census"] = st.checkbox(
        "Link to the 2016 census?",
        value=True,
        help="""If a dwelling in registered in the 2016 has had a BER assessment fill
        the building with its corresponding properties, else leave empty""",
    )
    selections["replace_not_stated"] = st.checkbox(
        "Replace 'Not stated' period built with Mode?",
        value=True,
        help="""Cannot archetype buildings with unknown Period Built so they must be
        inferred to estimate the properties of these buildings""",
    )
    selections["archetype"] = st.checkbox(
        "Fill unknown buildings with 'small area | countyname | period built' archetypes?",
        value=False,
        help="""If >30 buildings of the same 'small area | period built' create an
        archetype, else do 'small area | period built', finally do 'period built'
        """,
    )
    selections["filters"], selections["bounds"] = _select_ber_filters()
    selections["download_filetype"] = st.selectbox(
        "Download format?",
        options=[".csv.gz", ".parquet"],
        help="You might need to install 7zip to unzip '.csv.gz' (see hints)",
    )
    if st.button("Generate Building Stock?"):
        generate_building_stock(selections=selections, data_dir=data_dir, config=config)


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
    ]
    selected_filters: List[str] = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
        help="lb = Lower Bound, ub = Upper Bound",
    )

    with st.beta_expander("Change BER Filter Bounds?"):
        c1, c2 = st.beta_columns(2)
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


if __name__ == "__main__":
    main()

from configparser import ConfigParser
import datetime
from pathlib import Path
from typing import Any
from typing import Dict

import pandas as pd
import streamlit as st

from ibsg import archetype
from ibsg import census
from ibsg import CONFIG
from ibsg import DEFAULTS
from ibsg import postcodes
from ibsg import small_areas
from ibsg import _DATA_DIR
from ibsg import _LOCAL

# workaround from streamlit/streamlit#400
STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"


DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
if not DOWNLOADS_PATH.is_dir():
    DOWNLOADS_PATH.mkdir()


def main(
    data_dir: Path = DOWNLOADS_PATH,
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
    selections["download_filetype"] = st.selectbox(
        "Download format?",
        options=[".csv.gz", ".parquet"],
        help="You might need to install 7zip to unzip '.csv.gz' (see hints)",
    )

    _generate_building_stock(selections=selections, data_dir=data_dir, config=config)


def _generate_building_stock(
    selections: Dict[str, Any], data_dir: Path, config: ConfigParser
):
    if selections["ber_granularity"] == "countyname":
        is_generate_stock_selected = st.button("Fetch Postcode BERs")
        if is_generate_stock_selected:
            bers = postcodes.main(selections=selections, config=config)
    else:
        small_area_bers_zipfile = st.file_uploader(
            "Upload Small Area BERs",
            type="zip",
        )
        is_generate_stock_selected = bool(small_area_bers_zipfile)
        if is_generate_stock_selected:
            bers = small_areas.main(
                small_area_bers_zipfile, selections=selections, config=config
            )
    if is_generate_stock_selected & selections["census"]:
        with st.spinner("Linking to census ..."):
            census_bers = census.main(bers, selections=selections)
        selected_bers = census_bers
        create_download_link(
            selected_bers,
            filename=f"bers_{datetime.date.today()}",
            suffix=selections["download_filetype"],
            data_dir=data_dir,
        )
    elif is_generate_stock_selected:
        selected_bers = bers
        create_download_link(
            selected_bers,
            filename=f"bers_{datetime.date.today()}",
            suffix=selections["download_filetype"],
            data_dir=data_dir,
        )


def create_download_link(df: pd.DataFrame, filename: str, suffix: str, data_dir: Path):
    with st.spinner(f"Saving '{filename}{suffix}' to disk..."):
        filepath = (data_dir / filename).with_suffix(suffix)
        if suffix in [".csv", ".csv.gz"]:
            df.to_csv(filepath, index=False)
        elif suffix == ".parquet":
            df.to_parquet(filepath)
        else:
            st.error(f"{suffix} is not currently supported!")
        st.markdown(f"[{filepath.name}](downloads/{filepath.name})")


if __name__ == "__main__":
    main()

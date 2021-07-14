import datetime
from pathlib import Path
from typing import Any
from typing import Dict

import pandas as pd
import streamlit as st

from ibsg import archetype
from ibsg import census
from ibsg import DEFAULTS
from ibsg import postcodes
from ibsg import small_areas


def main(defaults: Dict[str, Any] = DEFAULTS):
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
    selections["archetype"] = st.checkbox(
        "Fill unknown buildings with 'small area | countyname | period built' archetypes?",
        value=True,
        help="""If >30 buildings of the same 'small area | period built' create an
        archetype, else do 'small area | period built', finally do 'period built'
        """,
    )
    selections["replace_not_stated"] = st.checkbox(
        "Replace 'Not stated' period built with Mode?",
        value=True,
        help="""Cannot archetype buildings with unknown Period Built so they must be
        inferred to estimate the properties of these buildings""",
    )
    selections["download_filetype"] = st.selectbox(
        "Download format?",
        options=[".csv.gz", ".parquet"],
        help="You might need to install 7zip to unzip '.csv.gz' (see hints)",
    )

    if selections["ber_granularity"] == "countyname":
        postcode_bers_is_selected = st.button("Fetch Postcode BERs")
        if postcode_bers_is_selected:
            _generate_countyname_building_stock(selections)

    elif selections["ber_granularity"] == "small_area":
        small_area_bers_zipfile = st.file_uploader(
            "Upload Small Area BERs",
            type="zip",
        )
        small_area_bers_is_selected = bool(small_area_bers_zipfile)
        if small_area_bers_is_selected:
            _generate_small_area_building_stock(
                selections, zipfile=small_area_bers_zipfile
            )


def _generate_countyname_building_stock(selections):
    st.info("'Link to 2016 census?' not yet implemented!")
    st.info("'Fill unknown buildings with archetypes?' not yet implemented!")
    postcode_bers = postcodes.main(selections=selections)
    create_download_link(
        postcode_bers,
        filename=f"postcode_bers_{datetime.date.today()}",
        suffix=selections["download_filetype"],
    )


def _generate_small_area_building_stock(selections, zipfile):
    small_area_bers = small_areas.main(zipfile, selections=selections)
    if selections["census"]:
        census_bers = census.main(small_area_bers, selections=selections)
    archetyped_bers, archetypes = archetype.main(census_bers, selections=selections)
    create_download_link(
        archetyped_bers,
        filename=f"small_area_bers_{datetime.date.today()}",
        suffix=selections["download_filetype"],
    )
    if archetypes:
        for name, data in archetypes.items():
            create_download_link(
                data,
                filename=f"{name}_archetypes_{datetime.date.today()}",
                suffix=".csv",
            )


def create_download_link(df: pd.DataFrame, filename: str, suffix: str):
    # workaround from streamlit/streamlit#400
    with st.spinner(f"Saving '{filename}{suffix}' to disk..."):
        STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
        DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
        if not DOWNLOADS_PATH.is_dir():
            DOWNLOADS_PATH.mkdir()
        filepath = (DOWNLOADS_PATH / filename).with_suffix(suffix)
        if suffix in [".csv", ".csv.gz"]:
            df.to_csv(filepath, index=False)
        elif suffix == ".parquet":
            df.to_parquet(filepath)
        else:
            st.error(f"{suffix} is not currently supported!")
        st.markdown(f"[{filepath.name}](downloads/{filepath.name})")


if __name__ == "__main__":
    main()

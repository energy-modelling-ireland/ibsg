import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from ibsg import archetype
from ibsg import census
from ibsg import DEFAULTS
from ibsg import postcodes
from ibsg import small_areas


def main():
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
        - You might need to install [7zip](https://www.7-zip.org/) to compress your
        closed-access small area BER dataset to a `zip` file under 200MB
        - `ibsg` won't be able to read your zipped Small Area BERs `csv` file if the
        column names don't match:

        `{list(DEFAULTS['small_areas']['mappings'].keys())}`
        """
        )

    c1, c2 = st.beta_columns(2)
    postcode_bers_is_selected = c1.button("Fetch Postcode BERs")
    small_area_bers_zipfile = c2.file_uploader(
        "Upload Small Area BERs",
        type="zip",
    )
    small_area_bers_is_selected = bool(small_area_bers_zipfile)
    census_is_selected = st.checkbox("Link to the 2016 census?", value=True)
    archetype_is_selected = st.checkbox(
        "Fill unknown buildings with archetypes?", value=True
    )

    if small_area_bers_is_selected:
        small_area_bers = small_areas.main(small_area_bers_zipfile)
        census_bers, census_is_selected = census.main(
            small_area_bers, archetype_is_selected
        )
        archetyped_bers, archetypes = archetype.main(
            census_bers, archetype_is_selected, census_is_selected
        )
        create_csv_download_link(
            archetyped_bers,
            filename=f"small_area_bers_{datetime.date.today()}",
            suffix=".csv.zip",
        )
        if archetypes:
            for name, data in archetypes.items():
                create_csv_download_link(
                    data,
                    filename=f"{name}_archetypes_{datetime.date.today()}",
                    suffix=".csv",
                )
    elif postcode_bers_is_selected:
        st.info("'Link to 2016 census?' not yet implemented!")
        st.info("'Fill unknown buildings with archetypes?' not yet implemented!")
        postcode_bers = postcodes.main()
        create_csv_download_link(
            postcode_bers,
            filename=f"postcode_bers_{datetime.date.today()}",
            suffix=".csv.gz",
            compression="gzip",
        )


def create_csv_download_link(df: pd.DataFrame, filename: str, suffix: str):
    # workaround from streamlit/streamlit#400
    with st.spinner(f"Saving '{filename}{suffix}' to disk..."):
        STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
        DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
        if not DOWNLOADS_PATH.is_dir():
            DOWNLOADS_PATH.mkdir()
        filepath = (DOWNLOADS_PATH / filename).with_suffix(suffix)
        df.to_csv(filepath, index=False)
        st.markdown(f"[{filepath.name}](downloads/{filepath.name})")


if __name__ == "__main__":
    main()

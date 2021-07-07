import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

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
        - You might need to install [7zip](https://www.7-zip.org/) for:
            - Compressing your closed-access small area BER dataset to a `zip` file of
            under 200MB
            - Open the `csv.gz` files 
        - `ibsg` won't be able to read your zipped Small Area BERs `csv` file if the
        column names don't match:

        `{list(DEFAULTS['small_areas']['mappings'].keys())}`
        """
        )

    c1, c2 = st.beta_columns(2)
    postcode_bers_selected = c1.button("Fetch Postcode BERs")
    small_area_bers_zipfile = c2.file_uploader(
        "Upload Small Area BERs",
        type="zip",
    )

    if small_area_bers_zipfile:
        small_area_bers = small_areas.main(small_area_bers_zipfile)
        create_csv_download_link(
            small_area_bers, f"small_area_bers_{datetime.date.today()}"
        )
    if postcode_bers_selected:
        postcode_bers = postcodes.main()
        create_csv_download_link(
            postcode_bers, f"postcode_bers_{datetime.date.today()}"
        )


def create_csv_download_link(df: pd.DataFrame, filename: str):
    # workaround from streamlit/streamlit#400
    st.markdown("Saving data as a `csv.gz` file...")
    STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
    DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
    if not DOWNLOADS_PATH.is_dir():
        DOWNLOADS_PATH.mkdir()
    filepath = (DOWNLOADS_PATH / filename).with_suffix(".csv.gz")
    df.to_csv(filepath, index=False, compression="gzip")
    st.markdown(f"[{filepath.name}](downloads/{filepath.name})")


if __name__ == "__main__":
    main()

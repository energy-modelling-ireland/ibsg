import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from ibsg import postcodes
from ibsg import small_areas


def main():
    st.header("🏠 Irish Building Stock Generator 🏠")
    st.markdown(
        """
        Generate a standardised building stock at postcode or small area level.

        - To compress your closed-access small area BER dataset to a `zip` file of
        under 200MB or to open the output file `csv.gz` you might need to install
        [7zip](https://www.7-zip.org/)

       
        If you have any problems or questions:
        - Chat with us on [Gitter](https://gitter.im/energy-modelling-ireland/ibsg)
        - Or raise an issue on our [Github](https://github.com/energy-modelling-ireland/ibsg) 
        """
    )

    postcode_bers_selected = st.button("Fetch Postcode BERs")
    small_area_bers_zipfile = st.file_uploader(
        "Or upload Small Area BERs",
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
            postcode_bers, f"small_area_bers_{datetime.date.today()}"
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

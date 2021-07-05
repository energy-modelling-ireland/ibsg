import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from ibsg import small_areas


def main():
    st.header("🏠 Irish Building Stock Generator 🏠")
    zipped_csv_of_bers = st.file_uploader(
        "Upload your zipped csv file of Small Area BERs",
        type="zip",
    )
    if zipped_csv_of_bers:
        small_area_bers = small_areas.main(zipped_csv_of_bers)
        save_to_csv_selected = st.button("Save to csv?")
        if save_to_csv_selected:
            ## Download
            _download_csv(
                df=small_area_bers,
                filename=f"ibsg_buildings_{datetime.date.today()}_small_area.csv",
            )


def _download_csv(df: pd.DataFrame, filename: str):
    # workaround from streamlit/streamlit#400
    STREAMLIT_STATIC_PATH = Path(st.__path__[0]) / "static"
    DOWNLOADS_PATH = STREAMLIT_STATIC_PATH / "downloads"
    if not DOWNLOADS_PATH.is_dir():
        DOWNLOADS_PATH.mkdir()
    df.to_csv(DOWNLOADS_PATH / filename, index=False)
    st.markdown(f"[{filename}](downloads/{filename})")


if __name__ == "__main__":
    main()

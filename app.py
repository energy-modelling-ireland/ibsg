import streamlit as st

from ibsg import io
from ibsg import postcodes
from ibsg import small_areas


def main():
    st.header("🏠 Irish Building Stock Generator 🏠")
    st.markdown(
        """
        Generate a standardised building stock at postcode or small area level.

        - To use the open-access postcode BERs enter your email address below to enable
        this application to make an authenticated query to the SEAI at:
        https://ndber.seai.ie/BERResearchTool/Register/Register.aspx
        - To compress your closed-access small area BER dataset to a `zip` file of
        under 200MB or to open the output file `csv.gz` you might need to install
        [7zip](https://www.7-zip.org/)

       
        If you have any problems or questions:
        - Chat with us on [Gitter](https://gitter.im/energy-modelling-ireland/ibsg)
        - Or raise an issue on our [Github](https://github.com/energy-modelling-ireland/ibsg) 
        """
    )

    small_area_bers_zipfile = st.file_uploader(
        "Upload Small Area BERs",
        type="zip",
    )
    email_address = st.text_input("Enter your email address")

    if small_area_bers_zipfile:
        small_area_bers = small_areas.main(small_area_bers_zipfile)
        io.download_as_csv(small_area_bers, category="small_area", engine="pandas")
    if email_address:
        postcode_bers = postcodes.main(email_address)
        io.download_as_csv(postcode_bers, category="postcode", engine="dask")


if __name__ == "__main__":
    main()

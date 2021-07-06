from pathlib import Path
from shutil import unpack_archive

from ber_api import request_public_ber_db
import dask.dataframe as dd
from dask.delayed import unzip
import fsspec
import icontract
from icontract import ViolationError
import pandas as pd
from stqdm import stqdm
import streamlit as st

from ibsg import clean
from ibsg import DEFAULTS
from ibsg import filter
from ibsg import io


def main(email_address: str) -> dd.DataFrame:
    ## Download
    zipped_filepath = Path.cwd() / "BERPublicsearch.zip"
    unzipped_filepath = Path.cwd() / "BERPublicsearch.txt"
    parquet_filepath = Path.cwd() / "BERPublicsearch.parquet"
    if not parquet_filepath.exists():
        st.write(
            f"Accessing {zipped_filepath.name}"
            " from https://ndber.seai.ie/BERResearchTool/Register/Register.aspx"
        )
        st.markdown("Unzipping data...")
        request_public_ber_db(email_address=email_address, tqdm_bar=stqdm)
        unpack_archive(zipped_filepath, extract_dir=Path.cwd())
        st.markdown("Converting data to `parquet` for faster re-runs...")
        postcode_bers_raw_txt = _load_postcode_bers(unzipped_filepath)
        postcode_bers_raw_txt.to_parquet(parquet_filepath)

    postcode_bers_raw = dd.read_parquet(parquet_filepath)

    with st.form("Apply Filters"):
        ## Filter
        postcode_bers_in_countyname = filter.filter_by_substrings(
            postcode_bers_raw,
            column_name="countyname",
            all_substrings=[
                "Co. Carlow",
                "Co. Cavan",
                "Co. Clare",
                "Co. Cork",
                "Co. Donegal",
                "Co. Dublin",
                "Co. Galway",
                "Co. Kerry",
                "Co. Kildare",
                "Co. Kilkenny",
                "Co. Laois",
                "Co. Leitrim",
                "Co. Limerick",
                "Co. Longford",
                "Co. Louth",
                "Co. Mayo",
                "Co. Meath",
                "Co. Monaghan",
                "Co. Offaly",
                "Co. Roscommon",
                "Co. Sligo",
                "Co. Tipperary",
                "Co. Waterford",
                "Co. Westmeath",
                "Co. Wexford",
                "Co. Wicklow",
                "Cork City",
                "Dublin 1",
                "Dublin 10",
                "Dublin 11",
                "Dublin 12",
                "Dublin 13",
                "Dublin 14",
                "Dublin 15",
                "Dublin 16",
                "Dublin 17",
                "Dublin 18",
                "Dublin 2",
                "Dublin 20",
                "Dublin 22",
                "Dublin 24",
                "Dublin 3",
                "Dublin 4",
                "Dublin 5",
                "Dublin 6",
                "Dublin 6W",
                "Dublin 7",
                "Dublin 8",
                "Dublin 9",
                "Galway City",
                "Limerick City",
                "Waterford City",
            ],
        )
        clean_postcode_bers = _clean_postcode_bers(postcode_bers_in_countyname)

        ## Submit
        st.form_submit_button(label="Re-apply Filters")

    return clean_postcode_bers


def _load_postcode_bers(filepath: Path) -> dd.DataFrame:
    dtype = DEFAULTS["postcodes"]["dtype"]
    mappings = DEFAULTS["postcodes"]["mappings"]
    return io.read(filepath, dtype=dtype, mappings=mappings, sep="\t", engine="dask")


def _clean_postcode_bers(bers: dd.DataFrame) -> pd.DataFrame:
    filter_names = [
        "Is not provisional",
        "0m² < ground_floor_area < 1000m²",
        "5% < living_area_percent < 90%",
        "main_sh_boiler_efficiency > 19%",
        "19% < main_hw_boiler_efficiency < 320%",
        "main_sh_boiler_efficiency_adjustment_factor > 0.7",
        "main_hw_boiler_efficiency_adjustment_factor > 0.7",
        "suppl_sh_heat_fraction ⊄ (0, 0.1, 0.15, 0.2)",
        "declared_loss_factor < 20",
        "0 < thermal_bridging_factor <= 0.15",
    ]
    selected_filters = st.multiselect(
        "Select Filters",
        options=filter_names,
        default=filter_names,
    )
    clean_bers = (
        bers.copy()
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="Is not provisional",
            selected_filters=selected_filters,
            condition="type_of_rating != 'Provisional    '",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="0m² < ground_floor_area < 1000m²",
            selected_filters=selected_filters,
            condition="ground_floor_area > 30 and ground_floor_area < 1000",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="5% < living_area_percent < 90%",
            selected_filters=selected_filters,
            condition="living_area_percent < 90 or living_area_percent > 5",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_sh_boiler_efficiency > 19%",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency > 19",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="19% < main_hw_boiler_efficiency < 320%",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency < 320 or main_hw_boiler_efficiency > 19",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_sh_boiler_efficiency_adjustment_factor > 0.7",
            selected_filters=selected_filters,
            condition="main_sh_boiler_efficiency_adjustment_factor > 0.7",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="main_hw_boiler_efficiency_adjustment_factor > 0.7",
            selected_filters=selected_filters,
            condition="main_hw_boiler_efficiency_adjustment_factor > 0.7",
        )
        .pipe(
            clean.get_rows_equal_to_values,
            filter_name="suppl_sh_heat_fraction ⊄ (0, 0.1, 0.15, 0.2)",
            selected_filters=selected_filters,
            on_column="suppl_sh_heat_fraction",
            values=["0", "0.1", "0.15", "0.2"],
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="declared_loss_factor < 20",
            selected_filters=selected_filters,
            condition="declared_loss_factor < 20",
        )
        .pipe(
            clean.get_rows_meeting_condition,
            filter_name="0 < thermal_bridging_factor <= 0.15",
            selected_filters=selected_filters,
            condition="thermal_bridging_factor > 0 or thermal_bridging_factor <= 0.15",
        )
    )
    length_before = len(bers)
    length_after = len(clean_bers)
    percentage_reduction = 100 * (length_before - length_after) / length_before
    st.write(f"⚠️Filtering removed {round(percentage_reduction, 2)}% of buildings ⚠️")
    return clean_bers

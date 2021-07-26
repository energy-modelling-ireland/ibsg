from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Dict

import streamlit as st
from streamlit import report_thread

from ibsg import archetype
from ibsg import ber
from ibsg import census
from ibsg import clean
from ibsg import merge


IS_STREAMLIT_THREAD = report_thread.get_report_ctx()


def generate_building_stock(
    data_dir: Path,
    filepath: Path,
    selections: Dict[str, Any],
    config: ConfigParser,
    defaults: Dict[str, Any],
) -> None:
    if selections["ber_granularity"] == "small_area":
        ber_buildings = ber.load_small_area_bers(
            zipped_csv=selections["small_area_bers"],
            dtype=defaults["small_areas"]["dtype"],
            mappings=defaults["small_areas"]["mappings"],
            filepath=data_dir / config["small_area_bers"]["filename"],
        ).pipe(ber.filter_bers, selections=selections, defaults=defaults)

    else:
        ber_buildings = ber.load_postcode_bers(
            url=config["postcode_bers"]["url"],
            filepath=data_dir / config["postcode_bers"]["filename"],
        ).pipe(ber.filter_bers, selections=selections, defaults=defaults)

    if selections["census"] & selections["archetype"]:
        if IS_STREAMLIT_THREAD:
            st.error(
                """
                Cannot fill the Census with BER Archetypes on the streamlit sharing
                free-tier resources!  Please deselect 'Fill Unknown Buildings with 
                Archetypes?' or build this App locally by following the 
                instructions on our Github.
                """
            )
            return
        else:
            buildings = (
                census.load_census_buildings(
                    url=config["census_buildings"]["url"],
                    filepath=data_dir / config["census_buildings"]["filename"],
                )
                .pipe(
                    clean.get_rows_containing_substrings,
                    column_name="countyname",
                    selected_substrings=selections["countyname"],
                    all_substrings=defaults["countyname"],
                )
                .pipe(census.replace_not_stated_period_built_with_mode)
                .pipe(
                    merge.fill_census_with_bers,
                    bers=ber_buildings,
                    ber_granularity=selections["ber_granularity"],
                )
                .pipe(
                    archetype.fillna_with_archetypes,
                    archetype_columns=[
                        [selections["ber_granularity"], "period_built"],
                        ["period_built"],
                    ],
                    sample_size=config["settings"]["sample_size"],
                )
            )
            del ber_buildings
    elif selections["census"]:
        buildings = (
            census.load_census_buildings(
                url=config["census_buildings"]["url"],
                filepath=data_dir / config["census_buildings"]["filename"],
            )
            .pipe(
                clean.get_rows_containing_substrings,
                column_name="countyname",
                selected_substrings=selections["countyname"],
                all_substrings=defaults["countyname"],
            )
            .pipe(census.replace_not_stated_period_built_with_mode)
            .pipe(
                merge.fill_census_with_bers,
                bers=ber_buildings,
                ber_granularity=selections["ber_granularity"],
            )
        )
        del ber_buildings
    else:
        buildings = ber_buildings

    if ".gz" == filepath.suffix:
        buildings.to_csv(filepath, index=False)
    elif "parquet" in filepath.suffix:
        buildings.to_parquet(filepath)
    else:
        raise ValueError(f"{filepath.suffix} is not supported by ibsg")

from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Dict

import streamlit as st

from ibsg import archetype
from ibsg import ber
from ibsg import census
from ibsg import clean
from ibsg import merge

STREAMLIT_SHARING = bool(st.secrets["STREAMLIT_SHARING"])


def generate_building_stock(
    data_dir: Path,
    filepath: Path,
    selections: Dict[str, Any],
    config: ConfigParser,
    defaults: Dict[str, Any],
) -> None:
    with st.spinner("Loading BERs..."):
        if selections["ber_granularity"] == "small_area":
            small_area_ids = ber.load_small_area_ids(
                url=config["small_area_ids"]["url"],
                filepath=data_dir / config["small_area_ids"]["filename"],
            )
            ber_buildings = ber.load_small_area_bers(
                zipped_csv=selections["small_area_bers"],
                dtype=defaults["small_areas"]["dtype"],
                mappings=defaults["small_areas"]["mappings"],
                filepath=data_dir / config["small_area_bers"]["filename"],
            ).pipe(
                ber.filter_bers,
                selections=selections,
                defaults=defaults,
                small_area_ids=small_area_ids,
            )

        else:
            ber_buildings = ber.load_postcode_bers(
                url=config["postcode_bers"]["url"],
                filepath=data_dir / config["postcode_bers"]["filename"],
            ).pipe(ber.filter_bers, selections=selections, defaults=defaults)

    if selections["census"] & selections["archetype"]:
        with st.spinner("Linking BERs to the 2016 Census & Filling with Archetypes..."):
            if STREAMLIT_SHARING:
                st.error(
                    """
                    Cannot fill the Census with BER Archetypes on 'streamlit sharing'
                    free-tier resources.  Please deselect 'Fill Unknown Buildings with 
                    Archetypes?' and try again, or follow the instructions on our
                    Github to run ibsg locally. 
                    """
                )
                return
            if selections["ber_granularity"] == "small_area":
                archetype_columns = [
                    ["small_area", "period_built"],
                    ["cso_ed_id", "period_built"],
                    ["countyname", "period_built"],
                    ["period_built"],
                ]
            else:
                archetype_columns = [
                    ["countyname", "period_built"],
                    ["period_built"],
                ]
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
                    archetype_columns=archetype_columns,
                    sample_size=config["settings"]["sample_size"],
                )
            )
            del ber_buildings
    elif selections["census"]:
        with st.spinner("Linking BERs to the 2016 Census..."):
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

    with st.spinner("Saving data..."):
        if ".gz" == filepath.suffix:
            buildings.to_csv(filepath, index=False)
        elif "parquet" in filepath.suffix:
            buildings.to_parquet(filepath)
        else:
            raise ValueError(f"{filepath.suffix} is not supported by ibsg")

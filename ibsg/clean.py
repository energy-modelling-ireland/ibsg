import json
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd

from ibsg import DEFAULTS


def standardise_ber_private_column_names(
    ber: pd.DataFrame,
    mappings: Optional[Dict[str, str]] = DEFAULTS["mappings"]["small_area_bers"],
):
    return ber.rename(columns=mappings)


def is_not_provisional(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`type_of_rating` != 'Provisional    '")


def is_valid_floor_area(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`ground_floor_area` > 30 and `ground_floor_area` < 1000")


def is_valid_living_area_percentage(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`living_area_percent` < 90 or `living_area_percent` > 5")


def is_valid_sh_boiler_efficiency(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("main_sh_boiler_efficiency > 19")


def is_valid_hw_boiler_efficiency(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query(
        "main_hw_boiler_efficiency < 320 or main_hw_boiler_efficiency > 19"
    )


def is_valid_sh_boiler_efficiency_adjustment_factor(
    ber: pd.DataFrame,
) -> pd.DataFrame:
    return ber.query("main_sh_boiler_efficiency_adjustment_factor > 0.7")


def is_valid_hw_boiler_efficiency_adjustment_factor(
    ber: pd.DataFrame,
) -> pd.DataFrame:
    return ber.query("main_hw_boiler_efficiency_adjustment_factor > 0.7")


def is_valid_suppl_boiler_efficiency_adjustment_factor(
    ber: pd.DataFrame,
) -> pd.DataFrame:
    return ber.query("suppl_sh_boiler_efficiency_adjustment_factor > 19")


def is_valid_small_area_id(
    ber: pd.DataFrame, small_area_ids: List[str]
) -> pd.DataFrame:
    return ber.query("small_area in @small_area_ids")

import json
from typing import Dict
from typing import Optional

import pandas as pd

from ibsg import DEFAULTS


def standardise_ber_private_column_names(
    ber: pd.DataFrame,
    mappings: Optional[Dict[str, str]] = DEFAULTS["mappings"]["BER Private"],
):
    return ber.rename(columns=mappings)


def is_not_provisional(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`type_of_rating` != 'Provisional    '")


def is_realistic_floor_area(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`ground_floor_area` > 30 and `ground_floor_area` < 1000")


def is_realistic_living_area_percentage(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query("`living_area_percent` < 90 or `living_area_percent` > 5")


def is_realistic_boiler_efficiency(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query(
        "`main_sh_boiler_efficiency` > 19"
        " or `main_hw_boiler_efficiency` < 320"
        " or `main_hw_boiler_efficiency` > 19"
    )


def is_realistic_boiler_efficiency_adjustment_factor(ber: pd.DataFrame) -> pd.DataFrame:
    return ber.query(
        "`main_sh_boiler_efficiency_adjustment_factor` > 0.7"
        " and `main_hw_boiler_efficiency_adjustment_factor` > 0.7"
        " and `suppl_sh_boiler_efficiency_adjustment_factor` > 19"
    )

"""Shuffle all confidential data."""

import pandas as pd


def _shuffle(s: pd.Series) -> pd.Series:
    return s.to_frame().sample(frac=1).values


def anonymise_small_area_bers(bers: pd.DataFrame):
    anonymised_bers = bers.copy()
    anonymised_bers["cso_small_area"] = _shuffle(bers["cso_small_area"])
    anonymised_bers["geo_small_area"] = _shuffle(anonymised_bers["geo_small_area"])
    anonymised_bers["UUID"] = _shuffle(anonymised_bers["UUID"])
    anonymised_bers["ED_Name"] = _shuffle(anonymised_bers["cso_small_area"])
    return anonymised_bers

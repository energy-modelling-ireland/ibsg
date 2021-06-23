from pathlib import Path
from typing import Dict, Optional

from ibsg import clean
from ibsg import io
from ibsg import gui


config = gui.ber_private()

print(f"Reading {config.InputFile}...")
ber_raw = io.read_ber_private(config.InputFile)
print("Read!")

print("Applying filters...")
ber_filtered = (
    ber_raw.pipe(clean.standardise_ber_private_column_names)
    .pipe(clean.is_not_provisional)
    .pipe(clean.is_realistic_floor_area)
    .pipe(clean.is_realistic_living_area_percentage)
    .pipe(clean.is_realistic_boiler_efficiency)
    .pipe(clean.is_realistic_boiler_efficiency_adjustment_factor)
    .pipe(clean.is_realistic_boiler_efficiency)
)
print("Done!")

savepath = Path(config.OutputDirectory) / "ber_private.csv"
print(f"Saving Processed Data to {savepath}...")
ber_filtered.to_csv(savepath, index=False)

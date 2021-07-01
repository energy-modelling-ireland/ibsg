POSTCODES = [
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
]


COUNTIES = [
    "Carlow",
    "Cavan",
    "Clare",
    "Cork",
    "Cork",
    "Donegal",
    "Dublin",
    "Galway",
    "Kerry",
    "Kildare",
    "Kilkenny",
    "Laois",
    "Leitrim",
    "Limerick",
    "Longford",
    "Louth",
    "Mayo",
    "Meath",
    "Monaghan",
    "Offaly",
    "Roscommon",
    "Sligo",
    "Tipperary",
    "Waterford",
    "Westmeath",
    "Wexford",
    "Wicklow",
]

DEFAULTS = {
    "dtype": {
        "UUID": "category",
        "cso_small_area": "category",
        "geo_small_area": "category",
        "CSO_ED_ID": "category",
        "ED_Name": "category",
        "prob_loc_error_0corr": "float32",
        "prob_loc_error_100corr": "float32",
        "CountyName2": "category",
        "Is public": "category",
        "Type of rating": "category",
        "Year of assessment": "int16",
        "Month of assessment": "category",
        "Valid until year": "int16",
        "Valid until month": "category",
        "Year of construction": "int16",
        "Month of construction": "category",
        "Year of construction range": "category",
        "Dwelling type description": "category",
        "Low energy fixed lighting": "int8",
        "Percentage Draught Stripped": "int8",
        "Thermal Mass Category Description": "category",
        "HS Main System Efficiency": "float32",
        "HS Eff Adj Factor": "float32",
        "HS Suppl Heat Fraction": "float32",
        "HS Suppl System Eff": "float32",
        "WH Main System Eff": "float32",
        "WH Eff Adj Factor": "float32",
        "Main SH Fuel Description": "category",
        "Suppl SH Fuel Description": "category",
        "Main WH Fuel Description": "category",
        "Suppl WH Fuel Description": "category",
        "SH Renewable Resources Description": "category",
        "WH Renewable Resources Description": "category",
        "Distribution Losses": "category",
        "Storage Losses": "category",
        "Solar Hot Water Heating": "category",
        "Renewable_electricity_generation": "category",
        "Elec Immersion In Summer": "category",
        "Water Storage Volume": "int16",
        "Temp Factor Unadj": "float32",
        "Temp Factor Multiplier": "float32",
        "Insulation Type Description": "category",
        "Insulation Thickness": "int16",
        "Primary Circuit Loss Description": "category",
        "Temperature Adjustment": "float32",
        "Heat System Control Cat": "category",
        "Heat System Response Cat": "category",
        "Total Physical Area": "int16",
        "No Of Storeys": "int32",
        "Living Area Percent": "float32",
        "Ground Floor Area": "float32",
        "First Floor Area": "float32",
        "Second Floor Area": "float32",
        "Third Floor Area": "float32",
        "Room in Roof Area": "float32",
        "Ground Floor Height": "float32",
        "First Floor Height": "float32",
        "Second Floor Height": "float32",
        "Third Floor Height": "float32",
        "Room In Roof Height": "float32",
        "Structure Type Description": "category",
        "No Of Chimneys": "float32",
        "No Of Open Flues": "float32",
        "No Of FansAndVents": "float32",
        "No Of Flueless GasFires": "float32",
        "Draft Lobby": "category",
        "Ventilation Method Description": "category",
        "Structure Type Description.1": "category",
        "Suspended Wooden Floor Description": "category",
        "Wall weighted Uvalue": "float32",
        "Door Weighted Uvalue": "float32",
        "Roof Weighted Uvalue": "float32",
        "Floor Weighted Uvalue": "float32",
        "WindowsWeighted Uvalue": "float32",
        "Wall Total Area": "float32",
        "Door Total Area": "float32",
        "Roof Total Area": "float32",
        "Floor Total Area": "float32",
        "Windows Total Area": "float32",
        "Energy Rating": "category",
        "Energy Value": "int32",
        "SHMainPrimaryEnergy": "float64",
        "SHSecPrimaryEnergy": "float64",
        "WHMainPrimaryEnergy": "float64",
        "WHSecPrimaryEnergy": "float64",
        "PumpsFansPrimaryEnergy": "float64",
        "LightingPrimaryEnergy": "float64",
        "CHPInputPrimaryEnergy": "float64",
        "CHPElecOutputPrimaryEnergy": "float64",
        "RenewPrimaryEnergy1": "float64",
        "RenewPrimaryEnergy2": "float64",
        "RenewPrimaryEnergy3": "float64",
        "SHMain2PrimaryEnergy": "float64",
        "RenewEPnren": "float64",
        "RenewEPren": "float64",
        "RenewPrimaryEPTot": "float64",
        "CO2 Value": "float64",
        "hlc": "float64",
        "Most Significant Wall Type": "category",
        "DeliveredLightingEnergy": "float64",
        "DeliveredEnergyPumpsFans": "float64",
        "DeliveredEnergyMainWater": "float64",
        "DeliveredEnergySupplementaryWater": "float64",
        "DeliveredSecondaryFraction": "float64",
        "DeliveredMainFraction": "float64",
        "DeliveredEnergyMainSpace": "float64",
        "DeliveredEnergySecondarySpace": "float64",
        "DeliveredEnergyPerM2FloorArea": "float64",
        "TotalDeliveredEnergy": "float64",
        "ElectricityPEFactor": "float32",
        "ElectricityCO2Factor": "float32",
    },
    "mappings": {
        "small_area_bers": {
            "cso_small_area": "small_area",
            "ED_Name": "electoral_district",
            "CountyName2": "countyname",
            "Type of rating": "type_of_rating",
            "Year of construction": "year_of_construction",
            "Dwelling type description": "dwelling_type",
            "Percentage Draught Stripped": "percentage_draught_stripped",
            "Thermal Mass Category Description": "thermal_mass_category",
            "HS Main System Efficiency": "main_sh_boiler_efficiency",
            "HS Eff Adj Factor": "main_sh_boiler_efficiency_adjustment_factor",
            "HS Suppl Heat Fraction": "suppl_sh_boiler_efficiency",
            "HS Suppl System Eff": "suppl_sh_boiler_efficiency_adjustment_factor",
            "WH Main System Eff": "main_hw_boiler_efficiency",
            "WH Eff Adj Factor": "main_hw_boiler_efficiency_adjustment_factor",
            "Main SH Fuel Description": "main_sh_boiler_fuel",
            "Suppl SH Fuel Description": "suppl_sh_boiler_fuel",
            "Main WH Fuel Description": "main_hw_boiler_fuel",
            "Suppl WH Fuel Description": "suppl_hw_boiler_fuel",
            "SH Renewable Resources Description": "sh_renewable_category",
            "WH Renewable Resources Description": "hw_renewable_category",
            "Distribution Losses": "is_distribution_losses",
            "Storage Losses": "is_storage_losses",
            "Solar Hot Water Heating": "is_solar_hw_heating",
            "Renewable_electricity_generation": "is_renewable_electricity",
            "Elec Immersion In Summer": "is_elec_immersion_in_summer",
            "Water Storage Volume": "water_storage_volume",
            "Temp Factor Unadj": "temp_factor_unadjusted",
            "Temp Factor Multiplier": "temp_factor_multiplier",
            "Insulation Type Description": "insulation_category",
            "Insulation Thickness": "insulation_thickness",
            "Primary Circuit Loss Description": "primary_circuit_loss_category",
            "Temperature Adjustment": "temperature_adjustment",
            "Heat System Control Cat": "heating_control_category",
            "Heat System Response Cat": "heating_response_category",
            "Total Physical Area": "total_physical_area",
            "No Of Storeys": "no_of_storeys",
            "Living Area Percent": "living_area_percent",
            "Ground Floor Area": "ground_floor_area",
            "First Floor Area": "first_floor_area",
            "Second Floor Area": "second_floor_area",
            "Third Floor Area": "third_floor_area",
            "Room in Roof Area": "room_in_roof_area",
            "Ground Floor Height": "ground_floor_height",
            "First Floor Height": "first_floor_height",
            "Second Floor Height": "second_floor_height",
            "Third Floor Height": "third_floor_height",
            "Room In Roof Height": "room_in_roof_height",
            "Structure Type Description": "structure_type_category_1",
            "No Of Chimneys": "no_of_chimneys",
            "No Of Open Flues": "no_of_open_flues",
            "No Of FansAndVents": "no_of_fans_and_vents",
            "No Of Flueless GasFires": "no_of_flueless_gas_fires",
            "Draft Lobby": "is_draft_lobby",
            "Ventilation Method Description": "ventilation_method_category",
            "Structure Type Description.1": "structure_type_category_2",
            "Suspended Wooden Floor Description": "is_suspended_wooden_floor",
            "Wall weighted Uvalue": "wall_uvalue",
            "Door Weighted Uvalue": "door_uvalue",
            "Roof Weighted Uvalue": "roof_uvalue",
            "Floor Weighted Uvalue": "floor_uvalue",
            "WindowsWeighted Uvalue": "window_uvalue",
            "Wall Total Area": "wall_area",
            "Door Total Area": "door_area",
            "Roof Total Area": "roof_area",
            "Floor Total Area": "floor_area",
            "Windows Total Area": "window_area",
            "Energy Rating": "energy_rating",
            "Energy Value": "energy_value",
            "SHMainPrimaryEnergy": "main_sh_primary_energy_1",
            "SHSecPrimaryEnergy": "suppl_sh_primary_energy",
            "WHMainPrimaryEnergy": "main_hw_primary_energy",
            "WHSecPrimaryEnergy": "suppl_hw_primary_energy",
            "PumpsFansPrimaryEnergy": "pumps_and_fans_primary_energy",
            "LightingPrimaryEnergy": "lighting_primary_energy",
            "CHPInputPrimaryEnergy": "chp_input_primary_energy",
            "CHPElecOutputPrimaryEnergy": "chp_elec_primary_energy",
            "RenewPrimaryEnergy1": "renewable_primary_energy_1",
            "RenewPrimaryEnergy2": "renewable_primary_energy_2",
            "RenewPrimaryEnergy3": "renewable_primary_energy_3",
            "SHMain2PrimaryEnergy": "main_sh_primary_energy_2",
            "CO2 Value": "co2",
            "hlc": "heat_loss_parameter",
            "Most Significant Wall Type": "wall_type_category",
            "DeliveredLightingEnergy": "lighting_delivered_energy",
            "DeliveredEnergyPumpsFans": "pumps_and_fans_delivered_energy",
            "DeliveredEnergyMainWater": "main_hw_delivered_energy",
            "DeliveredEnergySupplementaryWater": "suppl_hw_delivered_energy",
            "DeliveredSecondaryFraction": "suppl_delivered_fraction",
            "DeliveredMainFraction": "main_delivered_energy",
            "DeliveredEnergyMainSpace": "main_sh_delivered_energy",
            "DeliveredEnergySecondarySpace": "suppl_sh_delivered_energy",
            "DeliveredEnergyPerM2FloorArea": "delivered_energy_per_m2",
            "TotalDeliveredEnergy": "delivered_energy",
            "ElectricityPEFactor": "elec_primary_energy_factor",
            "ElectricityCO2Factor": "elec_co2_factor",
        }
    },
}

import asyncio
import data_collection.elexon_interaction as elexon_interaction
import ancillary_files.excel_interaction as excel_interaction
import ancillary_files.datetime_functions as datetime_functions
import pandas as pd
import gb_analysis.bm_analysis as bm_analysis
import gb_analysis.carbon_emissions as carbon_emissions
from gb_analysis.recalculate_niv import get_bsc_id_to_npt_mapping
import gb_analysis.recalculate_imbalance_cashflows as recalculate_imbalance_cashflows
from elexonpy.api_client import ApiClient
from  data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
from gb_analysis.system_price_from_stack import get_new_system_prices_by_date_and_period

output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/NIV Analysis'
output_filename = '2021 - Nov 2024 Analysis'
inputs_folder = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Results'
filename = 'Wind Output Analysis'
bm_units_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BM_Units.json'
ci_by_fuel_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Carbon Intensity by Fuel Type.xlsx'
supporting_data_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data'
ci_filename = 'FINAL - BMU to CI Mapping'
bsc_roles_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
mr1b_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Data/Elexon/MR1B Excel Reports'
system_prices_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/System Prices.xlsx'
cashflow_results_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Cashflow Results'


years = [2021, 2022, 2023, 2024]

async def main():
    recalculate_imbalance_cashflows.calculate_cashflows_from_excel(
        bsc_roles_filepath,
        True,
        True,
        True,
        mr1b_directory,
        system_prices_filepath,
        cashflow_results_directory,
        'Test'
    )
    
    

asyncio.run(main())
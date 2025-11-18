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
import visualisation.plots_from_excel as plots_from_excel
import data_processing.price_data_processing as price_data_processing
from gb_analysis.calculate_npt_profit import calculate_npt_welfare_from_id_prices
import p462_analysis.engine as engine
import isem_analysis.api_interaction as api_interaction
import isem_analysis.engine as isem_engine
import isem_analysis.plotting as plotting

output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Outlook/Investigations/P462 Mod/Code Testing'
output_filename = '2021 - Nov 2024 Analysis'
inputs_folder = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Results'
filename = 'Wind Output Analysis'
bm_units_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BM_Units.json'
ci_by_fuel_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Carbon Intensity by Fuel Type.xlsx'
supporting_data_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data'
imbalance_input_data_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Aggregated Results/Imbalance Results.xlsx'
system_price_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/System Prices.xlsx'
figures_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Figures'
filename = 'SP Histograms'
url = 'https://reports.sem-o.com/documents/EF_PT_ALL_20250902_20250903_BALIMB_INDIC_20250903T144659.XML'

years = [2024]

async def main():
    await bm_analysis.get_bm_volume_by_ccgt(
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/Side Projects/BM Volumes',
        'CCGT Bid Offer Volume',
        [2024]
    )
    
asyncio.run(main())
import asyncio
import data_collection.elexon_interaction as elexon_interaction
import ancillary_files.excel_interaction as excel_interaction
import ancillary_files.datetime_functions as datetime_functions
import pandas as pd
from elexonpy.api_client import ApiClient
from  data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
from gb_analysis.system_price_from_stack import get_new_system_prices_by_date_and_period

tlms_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Winter 2022 TLMs.xlsx'

async def main():
    excel_interaction.aggregate_results_to_one_file(
        folder = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Results',
        output_directory='/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Aggregated Results',
        output_filename='Full Aggregated Results'
    )
    

asyncio.run(main())
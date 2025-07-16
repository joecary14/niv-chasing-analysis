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
    api_client = ApiClient()
    missing_data_points = set()
    settlement_dates_with_periods_per_day = datetime_functions.get_settlement_dates_and_settlement_periods_per_day('2024-11-01', '2024-11-30')
    tlms_by_bmu = excel_interaction.create_dict_from_excel(tlms_filepath, 'BM Unit ID', 'TLM')
    ancillary_price_data_for_sp_calculation = await get_ancillary_price_data_for_sp_calculation(
        api_client, settlement_dates_with_periods_per_day, missing_data_points
    )
    full_ascending_settlement_stack_by_date_and_period = await elexon_interaction.get_full_settlement_stacks_by_date_and_period(api_client, settlement_dates_with_periods_per_day, missing_data_points)
    niv_data = await elexon_interaction.get_niv_data(settlement_dates_with_periods_per_day, api_client)
    niv_data['counterfactual_niv'] = niv_data['net_imbalance_volume']
    results = get_new_system_prices_by_date_and_period(
        full_ascending_settlement_stack_by_date_and_period,
        ancillary_price_data_for_sp_calculation,
        tlms_by_bmu,
        niv_data
    )
    
    # Save the results dataframe to Excel
    results.to_excel('/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/system_price_results.xlsx', index=False)
    return results

asyncio.run(main())
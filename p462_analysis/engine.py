import calendar
import datetime

import pandas as pd

import ancillary_files.datetime_functions as datetime_functions
import ancillary_files.excel_interaction as excel_interaction
import data_collection.elexon_interaction as elexon_interaction
import gb_analysis.recalculate_balancing_cashflows as recalculate_balancing_cashflows
import p462_analysis.settlement_stack_handler as settlement_stack_handler

from elexonpy.api_client import ApiClient
from data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
from p462_analysis.system_price_from_stack import get_new_system_prices_by_date_and_period 

async def run(
    years: list[int],
    months: list[int],
    output_directory: str,
    tlms_filepath: str,
) -> None:
    api_client = ApiClient()
    missing_data_points = set()
    system_prices = []
    balancing_costs = []
    tlms_by_bmu = excel_interaction.create_dict_from_excel(tlms_filepath, 'BM Unit ID', 'TLM')
    bm_units = pd.read_json('/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BM_Units.json')
    wind_bmu_ids = set(bm_units[bm_units['fuelType'] == 'WIND']['elexonBmUnit'].to_list())
    for year in years:
        for month in months:
            await run_by_month(
                year,
                month,
                api_client,
                missing_data_points,
                wind_bmu_ids,
                tlms_by_bmu,
                system_prices,
                balancing_costs,
                output_directory
            )
                
    system_prices_df = pd.concat(system_prices)
    balancing_costs_df = pd.concat(balancing_costs)
    sheet_names_dict = {
        'System Prices': system_prices_df,
        'Balancing Costs': balancing_costs_df
    }
    file_name = f'{years[0]}-{months[0]}_to_{years[-1]}-{months[-1]}_results'
    excel_interaction.dataframes_to_excel(sheet_names_dict.values(), output_directory, file_name, sheet_names_dict.keys())
    print("Completed all months")
    print(f"Missing data points: {len(missing_data_points)}")
    
    
async def run_by_month(
    year: int,
    month: int,
    api_client: ApiClient,
    missing_data_points: set[tuple[str, int]],
    wind_bmu_ids: set[str],
    tlms_by_bmu: dict[str, float],
    system_prices: list[pd.DataFrame],
    balancing_costs: list[pd.DataFrame],
    output_directory: str
) -> None:
    month_start_date = datetime.date(year, month, 1).strftime('%Y-%m-%d')
    _, last_day = calendar.monthrange(year, month)
    month_end_date = datetime.date(year, month, last_day).strftime('%Y-%m-%d')
    settlement_dates_with_periods_per_day = datetime_functions.get_settlement_dates_and_settlement_periods_per_day(month_start_date, month_end_date)
    full_ascending_settlement_stack_by_date_and_period = await elexon_interaction.get_full_settlement_stacks_by_date_and_period(api_client, settlement_dates_with_periods_per_day, missing_data_points)
    ancillary_price_data_for_sp_calculation = await get_ancillary_price_data_for_sp_calculation(api_client, settlement_dates_with_periods_per_day, missing_data_points)
    wind_sell_price_adjustment_by_date_and_period = {}
    new_settlement_stacks_by_date_and_period = settlement_stack_handler.recalculate_settlement_stacks(
        full_ascending_settlement_stack_by_date_and_period,
        wind_bmu_ids,
        wind_sell_price_adjustment_by_date_and_period
    )
    # Add wind_sell_price_adjustment column to ancillary_price_data
    ancillary_price_data = ancillary_price_data_for_sp_calculation.copy()
    ancillary_price_data['wind_sell_price_adjustment'] = ancillary_price_data.apply(
        lambda row: wind_sell_price_adjustment_by_date_and_period.get((row['settlement_date'].strftime('%Y-%m-%d'), row['settlement_period']), 0),
        axis=1
    )
    ancillary_price_data['new_sell_price_price_adjustment'] = ancillary_price_data['sell_price_price_adjustment'] + ancillary_price_data['wind_sell_price_adjustment']
    
    
    new_system_prices_by_date_and_period_df = get_new_system_prices_by_date_and_period(
        new_settlement_stacks_by_date_and_period,
        ancillary_price_data,
        tlms_by_bmu
    )
    new_system_prices_by_date_and_period_df['settlement_date'] = pd.to_datetime(new_system_prices_by_date_and_period_df['settlement_date']).dt.date
    
    # Recalculate balancing cashflows
    balancing_costs_df = recalculate_balancing_cashflows.calculate_balancing_costs(full_ascending_settlement_stack_by_date_and_period, new_settlement_stacks_by_date_and_period)
    system_price_df = await elexon_interaction.get_niv_data(settlement_dates_with_periods_per_day, api_client)
    recalculated_system_prices = system_price_df.merge(
        new_system_prices_by_date_and_period_df, on=['settlement_date', 'settlement_period'], 
        how='outer'
    )
    recalculated_system_prices = recalculated_system_prices.drop(columns=['start_time'])
    
    system_prices_df = excel_interaction.process_df_for_output(recalculated_system_prices, missing_data_points)
    balancing_costs_df = excel_interaction.process_df_for_output(balancing_costs_df, missing_data_points)
    
    
    sheet_names_dict = {
        'System Prices': system_prices_df,
        'Balancing Costs': balancing_costs_df,
    }
    
    excel_interaction.dataframes_to_excel(
        sheet_names_dict.values(), output_directory, f'{year}-{month}', sheet_names_dict.keys())
    print(f"Completed {year}-{month}")
    
    system_prices.append(recalculated_system_prices)
    balancing_costs.append(balancing_costs_df)
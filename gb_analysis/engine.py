import datetime
import calendar

import pandas as pd
import data_collection.elexon_interaction as elexon_interaction
from  data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
import gb_analysis.recalculate_niv as recalculate_niv
import gb_analysis.recalculate_settlement_stack as recalculate_settlement_stack
from gb_analysis.system_price_from_stack import get_new_system_prices_by_date_and_period 
import ancillary_files.datetime_functions as datetime_functions

from ancillary_files import excel_interaction
from elexonpy.api_client import ApiClient
#TODO - add in intraday stats
async def run(
    years: list,
    months: list, 
    output_directory: str, 
    file_name: str,
    bsc_roles_filepath: str,
    strict_npt: bool
) -> None:
    system_prices = []
    system_imbalances = []
    balancing_costs = []
    original_balancing_revenue = []
    new_balancing_revenue = []
    so_cashflows = []
    supplier_cashflows = []
    generator_cashflows = []
    mr1b_filepaths = excel_interaction.get_excel_filepaths('/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Data/Elexon/MR1B Excel Reports')
    filepath_dict = excel_interaction.create_filepath_dict(mr1b_filepaths)
    bsc_roles_to_npt_mapping = recalculate_niv.get_bsc_roles_to_npt_mapping(bsc_roles_filepath, strict_npt)
    for year in years:
        for month in months:
            year_month = datetime.date(year, month, 1).strftime('%Y-%m')
            mr1b_filepath = filepath_dict[year_month.replace('-', '_')]
            await run_by_month(month, year, bsc_roles_to_npt_mapping, mr1b_filepath, output_directory,
                               system_prices, system_imbalances, balancing_costs, original_balancing_revenue, new_balancing_revenue,
                               so_cashflows, supplier_cashflows, generator_cashflows)
    
    system_prices_df = excel_interaction.order_df_by_settlement_date_and_period_for_output(pd.concat(system_prices))
    system_imbalances_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(system_imbalances))
    balancing_costs_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(balancing_costs))
    original_balancing_revenue_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(original_balancing_revenue))
    new_balancing_revenue_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(new_balancing_revenue))
    so_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(so_cashflows))
    supplier_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(supplier_cashflows))
    generator_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(pd.concat(generator_cashflows))
    
    sheet_names_dict = {
        'System Prices': system_prices_df,
        'System Imbalances': system_imbalances_df,
        'Balancing Costs': balancing_costs_df,
        'Original Balancing Revenue': original_balancing_revenue_df,
        'New Balancing Revenue': new_balancing_revenue_df,
        'SO Cashflows': so_cashflows_df,
        'Supplier Cashflows': supplier_cashflows_df,
        'Generator Cashflows': generator_cashflows_df
    }
    excel_output.dataframes_to_excel(sheet_names_dict.values(), output_directory, file_name, sheet_names_dict.keys())
    print("Completed all months")          
    
async def run_by_month(
    month: int, 
    year: int, 
    bsc_roles_to_npt_mapping : dict[str, bool], 
    mr1b_filepath: str, 
    output_directory: str, 
    system_prices: list, 
    system_imbalances: list, 
    balancing_costs: list, 
    original_balancing_revenue: list, 
    new_balancing_revenue: list, 
    so_cashflows: list, 
    supplier_cashflows: list, 
    generator_cashflows: list
) -> None:
    month_start_date = datetime.date(year, month, 1).strftime('%Y-%m-%d')
    _, last_day = calendar.monthrange(year, month)
    month_end_date = datetime.date(year, month, last_day).strftime('%Y-%m-%d')
    settlement_dates_with_periods_per_day = datetime_functions.get_settlement_dates_and_settlement_periods_per_day(
        month_start_date, month_end_date, True)
    settlement_dates_and_periods_list = datetime_functions.get_list_of_settlement_dates_and_periods(
        settlement_dates_with_periods_per_day)
    api_client = ApiClient()
    
    # Fetch data
    #TODO - work out the TLMs adjustments
    tlms_by_bmu = {}
    # tlms_by_bmu = await data_downloader.download_tlms_by_bmu()
    full_ascending_settlement_stack_by_date_and_period = await elexon_interaction.get_full_settlement_stacks_by_date_and_period(api_client, settlement_dates_with_periods_per_day)
    
    # Recalculate imbalance, stack, and system price
    system_imbalance_with_and_without_npts_df = await recalculate_niv.recalculate_niv(year, month, mr1b_filepath, bsc_roles_to_npt_mapping, output_directory)
    new_settlement_stacks_by_date_and_period = await recalculate_settlement_stack.recalculate_stacks(api_client, 
                                                    settlement_dates_with_periods_per_day, system_imbalance_with_and_without_npts_df, full_ascending_settlement_stack_by_date_and_period)
    ancillary_price_data_for_sp_calculation = await get_ancillary_price_data_for_sp_calculation()
    new_system_prices_by_date_and_period_df = get_new_system_prices_by_date_and_period(
        new_settlement_stacks_by_date_and_period, ancillary_price_data_for_sp_calculation, tlms_by_bmu, system_imbalance_with_and_without_npts_df)
    system_price_df = await data_downloader.download_system_price_data_for_comparison(settlement_dates_and_periods_list)
    recalculated_system_prices = system_price_df.merge(
        new_system_prices_by_date_and_period_df, on=[ct.ColumnHeaders.DATE_PERIOD_PRIMARY_KEY.value], how='outer'
    )
    
    # Recalculate balancing cashflows
    balancing_costs_df = recalculate_balancing_cashflows.calculate_balancing_costs(full_ascending_settlement_stack_by_date_and_period, new_settlement_stacks_by_date_and_period)
    original_balancing_revenue_by_group_df, new_balancing_revenue_by_group_df = recalculate_balancing_cashflows.get_original_and_new_balancing_revenue_dfs(
        full_ascending_settlement_stack_by_date_and_period, new_settlement_stacks_by_date_and_period)
    
    # Recalculate imbalance cashflows
    mr1b_data_df = await data_downloader.get_mr1b_data(settlement_dates_with_periods_per_day.keys())
    so_cashflows_df = await recalculate_imbalance_cashflows.get_recalculated_imbalance_cashflows_SO(new_system_prices_by_date_and_period_df, mr1b_data_df)
    supplier_cashflows_df = await recalculate_imbalance_cashflows.recalculate_imbalance_cashflows_by_bsc_party_type('ts', new_system_prices_by_date_and_period_df, mr1b_data_df)
    generator_cashflows_df = await recalculate_imbalance_cashflows.recalculate_imbalance_cashflows_by_bsc_party_type('tg', new_system_prices_by_date_and_period_df, mr1b_data_df)
    
    #Output Monthly Data
    system_prices_df = excel_output.order_df_by_settlement_date_and_period_for_output(recalculated_system_prices)
    system_imbalances_df = excel_output.order_df_by_settlement_date_and_period_for_output(system_imbalance_with_and_without_npts_df)
    balancing_costs_df = excel_output.order_df_by_settlement_date_and_period_for_output(balancing_costs_df)
    original_balancing_revenue_df = excel_output.order_df_by_settlement_date_and_period_for_output(original_balancing_revenue_by_group_df)
    new_balancing_revenue_df = excel_output.order_df_by_settlement_date_and_period_for_output(new_balancing_revenue_by_group_df)
    so_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(so_cashflows_df)
    supplier_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(supplier_cashflows_df)
    generator_cashflows_df = excel_output.order_df_by_settlement_date_and_period_for_output(generator_cashflows_df)
    
    sheet_names_dict = {
        'System Prices': system_prices_df,
        'System Imbalances': system_imbalances_df,
        'Balancing Costs': balancing_costs_df,
        'Original Balancing Revenue': original_balancing_revenue_df,
        'New Balancing Revenue': new_balancing_revenue_df,
        'SO Cashflows': so_cashflows_df,
        'Supplier Cashflows': supplier_cashflows_df,
        'Generator Cashflows': generator_cashflows_df
    }
    excel_output.dataframes_to_excel(
        sheet_names_dict.values(), output_directory, f'{year}-{month}', sheet_names_dict.keys())
    print(f"Completed {year}-{month}")
    
    # Append to lists
    system_prices.append(recalculated_system_prices)
    system_imbalances.append(system_imbalance_with_and_without_npts_df)
    balancing_costs.append(balancing_costs_df)
    original_balancing_revenue.append(original_balancing_revenue_by_group_df)
    new_balancing_revenue.append(new_balancing_revenue_by_group_df)
    so_cashflows.append(so_cashflows_df)
    supplier_cashflows.append(supplier_cashflows_df)
    generator_cashflows.append(generator_cashflows_df)
    

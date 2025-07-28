import pandas as pd
import data_collection.elexon_interaction as elexon_interaction
import ancillary_files.datetime_functions as datetime_functions
import ancillary_files.excel_interaction as excel_interaction
import calendar

from elexonpy.api_client import ApiClient

async def get_bm_offer_volume_by_wind(
    output_file_directory: str,
    output_file_name: str,
    years: list[int]
) -> None:
    api_client = ApiClient()
    bm_units = pd.read_json('/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BM_Units.json')
    wind_bmu_set = set(bm_units[bm_units['fuelType'] == 'WIND']['elexonBmUnit'].to_list())
    all_results = []
    for year in years:
        for month in range(1, 13):
            month_start_date = f"{year}-{month:02d}-01"
            month_end_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"
            settlement_dates_with_periods_per_day = datetime_functions.get_settlement_dates_and_settlement_periods_per_day(
                month_start_date,
                month_end_date
            )
            offer_stacks = await elexon_interaction.get_accepted_offers_by_date_and_period(
                api_client,
                settlement_dates_with_periods_per_day
            )
            results = [
                calculate_wind_offer_and_all_volume_one_period(
                offer_stack, wind_bmu_set
                ) for offer_stack in offer_stacks
            ]
            results_df = pd.DataFrame(
                results,
                columns=[
                    'settlement_date',
                    'settlement_period',
                    'wind_accepted_offer_volume',
                    'all_accepted_offer_volume',
                    'wind_offer_percentage'
                ]
            )
            all_results.append(results_df)
            print(f"Calculated wind offer volume for {year}-{month:02d}")
    
    all_results_df = pd.concat(all_results, ignore_index=True)
    excel_interaction.dataframes_to_excel([all_results_df], output_file_directory, output_file_name)

def calculate_wind_offer_and_all_volume_one_period(
    offer_stack: pd.DataFrame,
    wind_bmu_set: set[str]
) -> tuple[str, int, float, float, float]:
    wind_offer_volume = offer_stack[offer_stack['id'].isin(wind_bmu_set)]['volume'].sum()
    all_offer_volume = offer_stack['volume'].sum()
    settlement_date = offer_stack.iloc[0]['settlement_date']
    settlement_period = offer_stack.iloc[0]['settlement_period']
    
    if all_offer_volume == 0:
        return settlement_date, settlement_period, 0.0, 0.0, None
    
    wind_offer_percentage = wind_offer_volume / all_offer_volume
    return (
        settlement_date,
        settlement_period,
        wind_offer_volume,
        all_offer_volume,
        wind_offer_percentage
    )

async def calculate_number_of_boa_before_settlement_period(
    output_file_directory: str,
    output_file_name: str,
    years: list[int]
) -> None:
    api_client = ApiClient()
    all_results = []
    for year in years:
        for month in range(1, 13):
            month_start_date = f"{year}-{month:02d}-01"
            month_end_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"
            settlement_dates_with_periods_per_day = datetime_functions.get_settlement_dates_and_settlement_periods_per_day(
                month_start_date,
                month_end_date
            )
            date_and_period_to_start_time = datetime_functions.get_settlement_date_period_to_utc_start_time_mapping(
                years
            )
            
            for settlement_date, settlement_dates_with_periods_per_day in settlement_dates_with_periods_per_day.items():
                bid_offer_acceptance_data_by_date_and_period = await elexon_interaction.get_bid_offer_acceptance_data(
                    settlement_date,
                    settlement_dates_with_periods_per_day,
                    api_client
                )
                
                pre_settlement_boa_count_df = get_number_of_boa_before_settlement_period(
                    date_and_period_to_start_time,
                    bid_offer_acceptance_data_by_date_and_period
                )
                
                all_results.append(pre_settlement_boa_count_df)
            print(f'Calculated pre-settlement BOA count for {year}-{month:02d}')
            
    all_results_df = pd.concat(all_results, ignore_index=True)
    excel_interaction.dataframes_to_excel([all_results_df], output_file_directory, output_file_name)

def get_number_of_boa_before_settlement_period(
    date_and_period_to_start_time: dict[tuple[str, int], str],
    bid_offer_acceptance_data_by_date_and_period: dict[tuple[str, int], pd.DataFrame]
) -> pd.DataFrame:
    results = []
    for (settlement_date, settlement_period), bid_offer_acceptance_data in bid_offer_acceptance_data_by_date_and_period.items():
        if bid_offer_acceptance_data.empty:
            continue
        start_time = date_and_period_to_start_time[(settlement_date, settlement_period)]
        pre_settlement_boa_count = bid_offer_acceptance_data[bid_offer_acceptance_data['acceptance_time'] < start_time].shape[0]
        all_acceptance_count = bid_offer_acceptance_data.shape[0]
        percent_acceptance = pre_settlement_boa_count / all_acceptance_count if all_acceptance_count > 0 else None
        results.append((settlement_date, settlement_period, pre_settlement_boa_count, all_acceptance_count, percent_acceptance))
    
    pre_settlement_boa_count_df = pd.DataFrame(results, columns=['settlement_date', 'settlement_period', 'pre_settlement_boa_count', 'all_acceptance_count', 'percent_acceptance'])
    
    return pre_settlement_boa_count_df
    
async def get_settlement_sate_and_period_to_start_time_mapping(
    settlement_dates_with_periods_per_day: dict[str, int],
    api_client: ApiClient
) -> dict[tuple[str, int], str]:
    auxiliary_niv_data = await elexon_interaction.get_niv_data(
    settlement_dates_with_periods_per_day,
    api_client
    )
    auxiliary_niv_data['settlement_date'] = pd.to_datetime(auxiliary_niv_data['settlement_date']).dt.strftime('%Y-%m-%d')
    settlement_date_and_period_to_start_time_dict = {
        (row['settlement_date'], row['settlement_period']): row['start_time']
        for _, row in auxiliary_niv_data.iterrows()
    }
    
    return settlement_date_and_period_to_start_time_dict

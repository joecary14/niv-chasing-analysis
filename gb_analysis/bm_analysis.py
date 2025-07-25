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

#TODO - finding out how many acceptances are pre-settlement period will require a different function as it pulls data from a different source (BOALF)

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
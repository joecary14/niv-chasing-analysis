import pandas as pd

from elexonpy.api_client import ApiClient

from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day
from ancillary_files.excel_interaction import dataframes_to_excel
from data_collection.elexon_interaction import get_niv_data


async def get_sp_and_reference_price_data(
    start_date: str,
    end_date: str,
    intraday_data_filepath: str,
    output_directory: str,
    api_client: ApiClient | None = None
) -> pd.DataFrame:
    settlement_dates_and_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(start_date, end_date)
    client = api_client or ApiClient()
    niv_data = await get_niv_data(settlement_dates_and_periods_per_day, client)
    niv_data['start_time'] = pd.to_datetime(niv_data['start_time'], utc=True)

    intraday_data = pd.read_csv(intraday_data_filepath)
    intraday_data['start_time'] = pd.to_datetime(intraday_data['start_time'], utc=True)

    combined_data = niv_data.merge(
        intraday_data[['start_time', 'vwap_1_2h']],
        on='start_time',
        how='left'
    )
    combined_data = combined_data.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)
    combined_data['start_time'] = combined_data['start_time'].dt.tz_localize(None)

    file_name = f'sp_and_reference_price_{start_date}_to_{end_date}'
    dataframes_to_excel([combined_data], output_directory, file_name, ['SP and Reference Price'])

    return combined_data



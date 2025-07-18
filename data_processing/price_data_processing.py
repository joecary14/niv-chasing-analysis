import pandas as pd

from data_collection.elexon_interaction import get_full_midp_data, get_price_adjustment_data
from elexonpy.api_client import ApiClient
from data_processing.stack_data_handler import check_missing_data

async def get_ancillary_price_data_for_sp_calculation(
    api_client: ApiClient,
    settlement_dates_with_periods_per_day: dict[str, int],
    missing_data = set[tuple[str, int]]
):
    mid_data = await get_market_index_price_data(settlement_dates_with_periods_per_day, api_client)
    missing_mid_data = check_missing_data(mid_data, settlement_dates_with_periods_per_day)
    missing_data.update(missing_mid_data)
    
    price_adjustment_data = await get_price_adjustment_data(['settlement_date', 'settlement_period', 'buy_price_price_adjustment', 'sell_price_price_adjustment'], settlement_dates_with_periods_per_day, api_client)
    if price_adjustment_data.empty: 
        combined_price_data = mid_data.copy()
        combined_price_data['buy_price_price_adjustment'] = 0
        combined_price_data['sell_price_price_adjustment'] = 0
    else:
        combined_price_data = mid_data.merge(
            price_adjustment_data,
            on=['settlement_date', 'settlement_period'],
            how='outer'
        )
        combined_price_data[['buy_price_price_adjustment', 'sell_price_price_adjustment']] = combined_price_data[['buy_price_price_adjustment', 'sell_price_price_adjustment']].fillna(0)
    
    return combined_price_data
    
async def get_market_index_price_data(
    settlement_dates_with_periods_per_day: dict[str, int], 
    api_client: ApiClient
) -> pd.DataFrame:
    combined_market_index_data = await get_full_midp_data(api_client, settlement_dates_with_periods_per_day)
    n2ex_data = combined_market_index_data[combined_market_index_data['data_provider'] == 'N2EXMIDP']
    apx_data = combined_market_index_data[combined_market_index_data['data_provider'] == 'APXMIDP']
    combined_market_index_data = pd.concat([n2ex_data, apx_data])
    combined_market_index_data['weighted_price'] = combined_market_index_data['price'] * combined_market_index_data['volume']
    grouped_data = combined_market_index_data.groupby(['settlement_date', 'settlement_period']).agg(
        total_volume=pd.NamedAgg(column='volume', aggfunc='sum'),
        total_weighted_price=pd.NamedAgg(column='weighted_price', aggfunc='sum')
    )
    grouped_data['vwap_midp'] = grouped_data['total_weighted_price'] / grouped_data['total_volume']
    result = grouped_data.reset_index()[['settlement_date', 'settlement_period', 'vwap_midp']]
    result = result.drop_duplicates()
    
    return result
import pandas as pd


from data_collection.elexon_interaction import get_full_midp_data
from elexonpy.api_client import ApiClient
from elexonpy.api.market_index_api import MarketIndexApi

#TODO - write a function which combines getting MIP and BPA/SPA
async def get_ancillary_price_data_for_sp_calculation():
    pass

async def get_market_index_price_data(
    settlement_dates: list[str], 
    api_client: ApiClient
) -> pd.DataFrame:
    combined_market_index_data = await get_full_midp_data(api_client, settlement_dates)
    n2ex_data = combined_market_index_data[combined_market_index_data['data_provider'] == 'n2ex_midp']
    apx_data = combined_market_index_data[combined_market_index_data['data_provider'] == 'apx_midp']
    combined_market_index_data = pd.concat([n2ex_data, apx_data])
    combined_market_index_data['weighted_price'] = combined_market_index_data['price'] * combined_market_index_data['volume']
    #TODO - rewrite to group by the settlement date and the settlement period
    grouped_data = combined_market_index_data.groupby().agg(
        total_volume=pd.NamedAgg(column='volume', aggfunc='sum'),
        total_weighted_price=pd.NamedAgg(column='weighted_price', aggfunc='sum')
    )
    grouped_data['vwap_midp'] = grouped_data['total_weighted_price'] / grouped_data['total_volume']
    result = grouped_data.reset_index()[['settlement_date', 'settlement_periox', 'vwap_midp']]
    
    return result
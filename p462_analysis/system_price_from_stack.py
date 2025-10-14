import pandas as pd
from gb_analysis.system_price_from_stack import get_new_system_price

def get_new_system_prices_by_date_and_period(
    new_settlement_stacks_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    ancillary_price_data: pd.DataFrame,
    tlm_by_bmu: dict[str, float]
) -> pd.DataFrame:
    system_prices = []
    ancillary_price_data_copy = ancillary_price_data.copy()
    ancillary_price_data_copy['settlement_date'] = pd.to_datetime(ancillary_price_data_copy['settlement_date']).dt.strftime('%Y-%m-%d')
    for (settlement_date, settlement_period), new_settlement_stack in new_settlement_stacks_by_date_and_period.items():
        price_data = ancillary_price_data_copy[
            (ancillary_price_data_copy['settlement_date'] == settlement_date) & 
            (ancillary_price_data_copy['settlement_period'] == settlement_period)
        ]
        if price_data.empty:
            system_prices.append((settlement_date, settlement_period, None))
            continue
        market_index_price = price_data['vwap_midp'].values[0]
        if new_settlement_stack.empty:
            system_prices.append((settlement_date, settlement_period, None))
            continue
        niv = new_settlement_stack['volume'].sum()
        price_adjustment_column_header = 'buy_price_price_adjustment' if niv > 0 else 'new_sell_price_price_adjustment'
        price_adjustment = price_data[price_adjustment_column_header].values[0]
        new_system_price = get_new_system_price(new_settlement_stack, price_adjustment, market_index_price, tlm_by_bmu, niv)
        system_prices.append((settlement_date, settlement_period, new_system_price))
    
    new_system_prices_df = pd.DataFrame(system_prices, columns=['settlement_date', 'settlement_period', 'recalculated_system_price'])
   
    return new_system_prices_df
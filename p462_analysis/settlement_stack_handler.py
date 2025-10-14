import pandas as pd

def recalculate_settlement_stacks(
    settlement_stacks_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    wind_bmu_ids: set[str],
    wind_sell_price_adjustment_by_date_and_period: dict[tuple[str, int], float],
    default_subsidy_bid_price: float = 0.0
) -> dict[tuple[str, int], pd.DataFrame]:
    recalculated_stacks = {}
    for key, settlement_stack_one_period in settlement_stacks_by_date_and_period.items():
        if settlement_stack_one_period.empty:
            recalculated_stacks[key] = settlement_stack_one_period
            continue
        df = settlement_stack_one_period.copy()
        sell_price_adjustment = get_sell_price_adjustment_from_wind_subsidy(wind_bmu_ids, df)
        wind_sell_price_adjustment_by_date_and_period[key] = sell_price_adjustment
        df.loc[(df['volume'] < 0) & (df['original_price'] < 0), 'original_price'] = default_subsidy_bid_price
        recalculated_stacks[key] = df
        
    return recalculated_stacks

def get_sell_price_adjustment_from_wind_subsidy(
    wind_bmu_ids: set[str],
    settlement_stack_one_period: pd.DataFrame
) -> float:
    wind_bmu_mask = settlement_stack_one_period['id'].isin(wind_bmu_ids)
    wind_bid_volume_accepted = settlement_stack_one_period.loc[wind_bmu_mask & (settlement_stack_one_period['volume'] < 0) & (settlement_stack_one_period['original_price'] < 0), 'volume'].sum()
    wind_bid_cost = (settlement_stack_one_period.loc[wind_bmu_mask & (settlement_stack_one_period['volume'] < 0), 'original_price'] * settlement_stack_one_period.loc[wind_bmu_mask & (settlement_stack_one_period['volume'] < 0), 'volume']).sum()
    sell_price_adjustment = wind_bid_cost / wind_bid_volume_accepted if wind_bid_volume_accepted != 0 else 0.0
    
    return sell_price_adjustment
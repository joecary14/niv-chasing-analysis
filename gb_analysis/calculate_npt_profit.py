import pandas as pd

def calculate(
    system_imbalance_with_and_without_npts_df: pd.DataFrame,
    ancillary_price_data_df: pd.DataFrame
) -> pd.DataFrame:
    npt_aggregate_imbalance = system_imbalance_with_and_without_npts_df['settlement_date', 'settlement_period'].copy()
    npt_aggregate_imbalance['npt_net_id_position'] = -system_imbalance_with_and_without_npts_df['npt_total_imbalance']
    mid_price_df = ancillary_price_data_df[['settlement_date', 'settlement_period', 'vwap_midp']].copy()
    npt_aggregate_imbalance = npt_aggregate_imbalance.merge(
        mid_price_df, 
        on=['settlement_date', 'settlement_period'], 
        how='left'
    )
    npt_aggregate_imbalance['id_cashflow'] = npt_aggregate_imbalance['npt_net_id_position'] * npt_aggregate_imbalance['vwap_midp']
    npt_aggregate_imbalance = npt_aggregate_imbalance[['settlement_date', 'settlement_period', 'id_cashflow']]
    
    return npt_aggregate_imbalance
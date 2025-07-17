import pandas as pd

def calculate_id_position(
    system_imbalance_with_and_without_npts_df: pd.DataFrame,
    ancillary_price_data_df: pd.DataFrame
) -> pd.DataFrame:
    npt_aggregate_imbalance = system_imbalance_with_and_without_npts_df[['settlement_date', 'settlement_period']].copy()
    npt_aggregate_imbalance['npt_net_id_position'] = -system_imbalance_with_and_without_npts_df['npt_total_imbalance']
    mid_price_df = ancillary_price_data_df[['settlement_date', 'settlement_period', 'vwap_midp']].copy()
    mid_price_df['settlement_date'] = pd.to_datetime(mid_price_df['settlement_date']).dt.strftime('%Y-%m-%d')
    npt_aggregate_imbalance = npt_aggregate_imbalance.merge(
        mid_price_df, 
        on=['settlement_date', 'settlement_period'], 
        how='outer'
    )
    npt_aggregate_imbalance['id_cashflow'] = npt_aggregate_imbalance['npt_net_id_position'] * npt_aggregate_imbalance['vwap_midp']
    npt_aggregate_imbalance = npt_aggregate_imbalance[['settlement_date', 'settlement_period', 'id_cashflow']]
    
    return npt_aggregate_imbalance

def calculate_npt_welfare(
    npt_cashflows_df: pd.DataFrame,
    npt_intraday_position: pd.DataFrame
) -> pd.DataFrame:
    npt_cashflows_df['settlement_date'] = pd.to_datetime(npt_cashflows_df['settlement_date']).dt.strftime('%Y-%m-%d')
    npt_intraday_position['settlement_date'] = pd.to_datetime(npt_intraday_position['settlement_date']).dt.strftime('%Y-%m-%d')
    npt_welfare = npt_cashflows_df.merge(
        npt_intraday_position, 
        on=['settlement_date', 'settlement_period'], 
        how='outer'
    )
    npt_welfare['npt_welfare'] = npt_welfare['npt_imbalance_cashflow'] + npt_welfare['id_cashflow']
    
    return npt_welfare

import pandas as pd

def get_recalculated_imbalance_cashflows_SO(
    recalculated_system_price_df: pd.DataFrame, 
    mr1b_data_df : pd.DataFrame,
    npt_ids: list[str]
) -> pd.DataFrame:
    current_net_cashflow_df = calculate_current_net_cashflow(mr1b_data_df) #Positive implies revenue for SO
    recalculated_mr1b_data = set_npt_imbalance_volume_to_zero(mr1b_data_df, npt_ids)
    recalculated_system_price_by_date_and_period = recalculated_system_price_df.set_index(
        ['settlement_date', 'settlement_period'])['recalculated_system_price'].to_dict()
    recalculated_imbalance_cashflows = recalculate_imbalance_cashflows_SO(recalculated_mr1b_data, recalculated_system_price_by_date_and_period)
    cashflows_df = current_net_cashflow_df.merge(recalculated_imbalance_cashflows, on=['settlement_date', 'settlement_period'])
    
    return cashflows_df

def calculate_current_net_cashflow(
    mr1b_data_df: pd.DataFrame
) -> pd.DataFrame:
    imbalance_cashflow_by_date_and_period = []
    mr1b_data_df_copy = mr1b_data_df.copy()
    mr1b_data_df_copy['settlement_date'] = pd.to_datetime(mr1b_data_df_copy['settlement_date']).dt.date
    for settlement_date, mr1b_data_by_date in mr1b_data_df_copy.groupby('settlement_date'):
        for settlement_period, mr1b_data_by_period in mr1b_data_by_date.groupby('settlement_period'):
            imbalance_cashflow = mr1b_data_by_period['imbalance_charge'].sum()
            imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, imbalance_cashflow))
    
    imbalance_cashflow_by_date_and_period_df = pd.DataFrame(imbalance_cashflow_by_date_and_period, columns=['settlement_date', 'settlement_period', 'energy_imbalance_cashflow'])
    
    return imbalance_cashflow_by_date_and_period_df
#TODO - add in sensitivity for setting imbalance to 0 onyl for those NPTs for which their metered volume is 0 (may need to do this earlier in the programme too)
def set_npt_imbalance_volume_to_zero(
    mr1b_raw_data: pd.DataFrame,
    npt_ids: list[str]
) -> pd.DataFrame:
    recalculated_mr1b_data = mr1b_raw_data.copy()
    npt_ids_set = set(npt_ids)
    recalculated_mr1b_data.loc[recalculated_mr1b_data['party_id'].isin(npt_ids_set), 'energy_imbalance_vol'] = 0
    
    return recalculated_mr1b_data

def recalculate_imbalance_cashflows_SO(
    recalculated_mr1b_data: pd.DataFrame, 
    recalculated_system_price_by_date_and_period : dict[tuple[str, int], float]
) -> pd.DataFrame:
    recalculated_imbalance_cashflow_by_date_and_period = []
    recalculated_mr1b_data['settlement_date'] = pd.to_datetime(recalculated_mr1b_data['settlement_date']).dt.date
    for settlement_date, recalclated_mr1b_data_by_settlement_date in recalculated_mr1b_data.groupby('settlement_date'):
        for settlement_period, recalculated_mr1b_data_by_settlement_period in recalclated_mr1b_data_by_settlement_date.groupby('settlement_period'):
            recalculated_system_price = recalculated_system_price_by_date_and_period[(settlement_date, settlement_period)]
            recalculated_imbalance_cashflow = -(recalculated_mr1b_data_by_settlement_period['energy_imbalance_vol']*recalculated_system_price).sum()
            recalculated_imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, recalculated_imbalance_cashflow))
    
    recalculated_imbalance_cashflow_by_date_and_period_df = pd.DataFrame(recalculated_imbalance_cashflow_by_date_and_period, columns=['settlement_date', 'settlement_period', 'recalculated_energy_imbalance_cashflow'])
    
    return recalculated_imbalance_cashflow_by_date_and_period_df
#TODO - change this so that we feed in the relevant party types
def recalculate_imbalance_cashflows_by_bsc_party_type(
    bsc_party_ids: list[str], 
    recalculated_system_price_df: pd.DataFrame, 
    mr1b_data_df : pd.DataFrame,
    npt_ids: list[str]
) -> pd.DataFrame:
    bsc_party_type_ids_set = set(bsc_party_ids)
    mr1b_data_by_party_type = mr1b_data_df[mr1b_data_df['party_id'].isin(bsc_party_type_ids_set)]
    recalculated_system_price_by_date_and_period = recalculated_system_price_df.set_index(
        ['settlement_date', 'settlement_period'])['recalculated_system_price'].to_dict()
    recalculated_imbalance_cashflow_by_date_and_period = get_old_and_new_cashflows_by_bsc_party_type(
        mr1b_data_by_party_type, recalculated_system_price_by_date_and_period, npt_ids)
    
    return recalculated_imbalance_cashflow_by_date_and_period

def get_old_and_new_cashflows_by_bsc_party_type(
    mr1b_data_by_party_type: pd.DataFrame, 
    recalculated_system_price_data_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    npt_ids: list[str]
) -> pd.DataFrame:
    recalculated_imbalance_cashflow_by_date_and_period = []
    mr1b_data_copy = mr1b_data_by_party_type.copy()
    mr1b_data_copy['settlement_date'] = pd.to_datetime(mr1b_data_copy['settlement_date']).dt.date
    recalculated_mr1b_data = set_npt_imbalance_volume_to_zero(mr1b_data_copy, npt_ids)
    for settlement_date, recalclated_mr1b_data_by_settlement_date in recalculated_mr1b_data.groupby('settlement_date'):
        for settlement_period, recalculated_mr1b_data_by_settlement_period in recalclated_mr1b_data_by_settlement_date.groupby('settlement_period'):
            recalculated_system_price = recalculated_system_price_data_by_date_and_period[(settlement_date, settlement_period)]
            original_imbalance_cashflow = -recalculated_mr1b_data_by_settlement_period['imbalance_charge'].sum() # - sign to ensure that positive is profit for party
            recalculated_imbalance_cashflow = (recalculated_mr1b_data_by_settlement_period['energy_imbalance_vol']*recalculated_system_price).sum()
            recalculated_imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, original_imbalance_cashflow, recalculated_imbalance_cashflow))
    
    recalculated_imbalance_cashflow_by_date_and_period_df = pd.DataFrame(recalculated_imbalance_cashflow_by_date_and_period, 
                                                                         columns=['settlement_date',
                                                                                  'settlement_period', 
                                                                                  'energy_imbalance_cashflow', 
                                                                                  'recalculated_energy_imbalance_cashflow'])
    
    return recalculated_imbalance_cashflow_by_date_and_period_df
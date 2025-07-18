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
    current_net_cashflow_df['settlement_date'] = pd.to_datetime(current_net_cashflow_df['settlement_date']).dt.strftime('%Y-%m-%d')
    cashflows_df = current_net_cashflow_df.merge(recalculated_imbalance_cashflows, on=['settlement_date', 'settlement_period'])
    
    return cashflows_df

def calculate_current_net_cashflow(
    mr1b_data_df: pd.DataFrame
) -> pd.DataFrame:
    imbalance_cashflow_by_date_and_period = []
    mr1b_data_df_copy = mr1b_data_df.copy()
    for settlement_date, mr1b_data_by_date in mr1b_data_df_copy.groupby('Settlement Date'):
        for settlement_period, mr1b_data_by_period in mr1b_data_by_date.groupby('Settlement Period'):
            imbalance_cashflow = mr1b_data_by_period['Imbalance Charge'].sum()
            imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, imbalance_cashflow))
    
    imbalance_cashflow_by_date_and_period_df = pd.DataFrame(imbalance_cashflow_by_date_and_period, columns=['settlement_date', 'settlement_period', 'energy_imbalance_cashflow'])
    
    return imbalance_cashflow_by_date_and_period_df

def set_npt_imbalance_volume_to_zero(
    mr1b_raw_data: pd.DataFrame,
    npt_ids: list[str]
) -> pd.DataFrame:
    recalculated_mr1b_data = mr1b_raw_data.copy()
    npt_ids_set = set(npt_ids)
    recalculated_mr1b_data.loc[recalculated_mr1b_data['Party ID'].isin(npt_ids_set), 'Energy Imbalance Vol'] = 0
    
    return recalculated_mr1b_data

def recalculate_imbalance_cashflows_SO(
    recalculated_mr1b_data: pd.DataFrame, 
    recalculated_system_price_by_date_and_period : dict[tuple[str, int], float]
) -> pd.DataFrame:
    recalculated_imbalance_cashflow_by_date_and_period = []
    for settlement_date, recalclated_mr1b_data_by_settlement_date in recalculated_mr1b_data.groupby('Settlement Date'):
        for settlement_period, recalculated_mr1b_data_by_settlement_period in recalclated_mr1b_data_by_settlement_date.groupby('Settlement Period'):
            if type(settlement_date) is not str: settlement_date = settlement_date.strftime('%Y-%m-%d')
            #TODO - test code
            if (settlement_date, settlement_period) not in recalculated_system_price_by_date_and_period:
                continue
            
            recalculated_system_price = recalculated_system_price_by_date_and_period[(settlement_date, settlement_period)]
            recalculated_imbalance_cashflow = -(recalculated_mr1b_data_by_settlement_period['Energy Imbalance Vol']*recalculated_system_price).sum()
            recalculated_imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, recalculated_imbalance_cashflow))
    
    recalculated_imbalance_cashflow_by_date_and_period_df = pd.DataFrame(recalculated_imbalance_cashflow_by_date_and_period, columns=['settlement_date', 'settlement_period', 'recalculated_energy_imbalance_cashflow'])
    
    return recalculated_imbalance_cashflow_by_date_and_period_df

def recalculate_imbalance_cashflows_by_bsc_party_type(
    bsc_party_ids_to_type_mapping: dict[str, bool], 
    recalculated_system_price_df: pd.DataFrame, 
    mr1b_data_df : pd.DataFrame,
    npt_ids: list[str]
) -> pd.DataFrame:
    mr1b_df_bsc_type_only = mr1b_data_df[mr1b_data_df['Party ID'].map(bsc_party_ids_to_type_mapping) == True]
    recalculated_system_price_by_date_and_period = recalculated_system_price_df.set_index(
        ['settlement_date', 'settlement_period'])['recalculated_system_price'].to_dict()
    recalculated_imbalance_cashflow_by_date_and_period = get_old_and_new_cashflows_by_bsc_party_type(
        mr1b_df_bsc_type_only, recalculated_system_price_by_date_and_period, npt_ids)
    
    return recalculated_imbalance_cashflow_by_date_and_period

def get_old_and_new_cashflows_by_bsc_party_type(
    mr1b_data_by_party_type: pd.DataFrame, 
    recalculated_system_price_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    npt_ids: list[str]
) -> pd.DataFrame:
    recalculated_imbalance_cashflow_by_date_and_period = []
    mr1b_data_copy = mr1b_data_by_party_type.copy()
    recalculated_mr1b_data = set_npt_imbalance_volume_to_zero(mr1b_data_copy, npt_ids)
    for settlement_date, recalclated_mr1b_data_by_settlement_date in recalculated_mr1b_data.groupby('Settlement Date'):
        if type(settlement_date) is not str: settlement_date = settlement_date.strftime('%Y-%m-%d')
        for settlement_period, recalculated_mr1b_data_by_settlement_period in recalclated_mr1b_data_by_settlement_date.groupby('Settlement Period'):
            #TODO - test code
            if (settlement_date, settlement_period) not in recalculated_system_price_by_date_and_period:
                continue
            recalculated_system_price = recalculated_system_price_by_date_and_period[(settlement_date, settlement_period)]
            original_imbalance_cashflow = -recalculated_mr1b_data_by_settlement_period['Imbalance Charge'].sum() # - sign to ensure that positive is profit for party
            recalculated_imbalance_cashflow = (recalculated_mr1b_data_by_settlement_period['Energy Imbalance Vol']*recalculated_system_price).sum()
            recalculated_imbalance_cashflow_by_date_and_period.append((settlement_date, settlement_period, original_imbalance_cashflow, recalculated_imbalance_cashflow))
    
    recalculated_imbalance_cashflow_by_date_and_period_df = pd.DataFrame(
        recalculated_imbalance_cashflow_by_date_and_period, 
        columns=['settlement_date', 'settlement_period', 'energy_imbalance_cashflow', 'recalculated_energy_imbalance_cashflow']
    )
    
    return recalculated_imbalance_cashflow_by_date_and_period_df

def calculate_net_npt_cashflow(
    bsc_party_id_to_npt_mapping: dict[str, bool],
    mr1b_data_df: pd.DataFrame
) -> pd.DataFrame:
    mr1b_df_npts_only = mr1b_data_df[mr1b_data_df['Party ID'].map(bsc_party_id_to_npt_mapping) == True]
    mr1b_data_copy = mr1b_df_npts_only.copy()
    mr1b_data_copy['Settlement Date'] = pd.to_datetime(mr1b_data_copy['Settlement Date']).dt.date
    imbalance_cashflow = []
    for settlement_date, recalclated_mr1b_data_by_settlement_date in mr1b_data_copy.groupby('Settlement Date'):
        for settlement_period, recalculated_mr1b_data_by_settlement_period in recalclated_mr1b_data_by_settlement_date.groupby(f'Settlement Period'):
            original_imbalance_cashflow = -recalculated_mr1b_data_by_settlement_period['Imbalance Charge'].sum() # - sign to ensure that positive is profit for party
            imbalance_cashflow.append((settlement_date, settlement_period, original_imbalance_cashflow))
    
    imbalance_cashflow_df = pd.DataFrame(imbalance_cashflow, columns=['settlement_date', 'settlement_period', 'npt_imbalance_cashflow'])

    return imbalance_cashflow_df
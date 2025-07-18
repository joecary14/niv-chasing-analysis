import pandas as pd

def calculate_marginal_emissions(
    full_ascending_settlement_stack_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    new_settlement_stacks_by_date_and_period: dict[tuple[str, int], pd.DataFrame],
    system_imbalance_with_and_without_npts_by_date_and_period: pd.DataFrame,
    bmu_id_to_ci_mapping: dict[str, float]
) -> pd.DataFrame:
    mefs = []
    for (settlement_date, settlement_period), settlement_stack in full_ascending_settlement_stack_by_date_and_period.items():
        if (settlement_date, settlement_period) not in new_settlement_stacks_by_date_and_period:
            mefs.append(settlement_date, settlement_period, None, None)
            continue
        
        new_stack = new_settlement_stacks_by_date_and_period[(settlement_date, settlement_period)]
        row = system_imbalance_with_and_without_npts_by_date_and_period.loc[
            (system_imbalance_with_and_without_npts_by_date_and_period['settlement_date'] == settlement_date) &
            (system_imbalance_with_and_without_npts_by_date_and_period['settlement_period'] == settlement_period)
        ]
        if row.empty:
            mefs.append((settlement_date, settlement_period, None, None))
            continue
        system_imbalance = row['net_imbalance_volume'].values[0]
        counterfactual_imbalance = row['counterfactual_niv'].values[0]
        
        factual_mef, factual_bmu_id = get_mef_and_bmu_id(system_imbalance, settlement_stack, bmu_id_to_ci_mapping)
        counterfactual_mef, counterfactual_bmu_id = get_mef_and_bmu_id(counterfactual_imbalance, new_stack, bmu_id_to_ci_mapping)
        
        mefs.append((settlement_date, settlement_period, factual_mef, factual_bmu_id, counterfactual_mef, counterfactual_bmu_id))
    
    mef_df = pd.DataFrame(mefs, columns=['settlement_date', 'settlement_period', 'factual_mef', 'factual_marginal_unit', 'counterfactual_mef', 'counterfactual_marginal_unit'])
    
    return mef_df
        

def get_mef_and_bmu_id(
    system_imbalance: float,
    settlement_stack: pd.DataFrame,
    bmu_id_to_ci_mapping: dict[str, float]
) -> float | None:
    if settlement_stack.empty:
        return None
    unflagged_settlement_stack = settlement_stack[settlement_stack['so_flag'] == False]
    if system_imbalance > 0:
        marginal_action = unflagged_settlement_stack.iloc[0]
    else:
        marginal_action = unflagged_settlement_stack.iloc[-1]
    
    marginal_action_bmu_id = marginal_action['id']
    
    if marginal_action_bmu_id in bmu_id_to_ci_mapping:
        mef = bmu_id_to_ci_mapping[marginal_action_bmu_id]
    else:
        mef = None
    
    return mef, marginal_action_bmu_id
import pandas as pd
import numpy as np

import data_processing.bm_unit as bm_unit
import data_processing.boa as boa

def get_bmus_one_period(grouped_bid_offer_data_one_period, full_settlement_stack_one_period, physical_volumes_by_bmu):
    grouped_full_settlement_stack = full_settlement_stack_one_period.groupby('ID')
    bmus = populate_bmu_objects_one_period(grouped_bid_offer_data_one_period, grouped_full_settlement_stack, physical_volumes_by_bmu)
    return bmus

def populate_bmu_objects_one_period(grouped_bid_offer_data_one_period, grouped_full_settlement_stack_one_period, physical_volumes_by_bmu):
    bmus = {}
    acceptance_volumes_by_bmu_by_pair = get_acceptance_volumes_by_bmu_by_pair(grouped_full_settlement_stack_one_period)
    for bm_unit_id, bid_offer_pairs_df in grouped_bid_offer_data_one_period:
        bid_offer_pairs = bid_offer_pairs_df.set_index(ct.ColumnHeaders.PAIR_ID.value).to_dict('index')
        factual_acceptance_volumes_by_pair = {pair_id: 0 for pair_id in bid_offer_pairs.keys()}
        if bm_unit_id in acceptance_volumes_by_bmu_by_pair:
            for pair_id, acceptance_volume in acceptance_volumes_by_bmu_by_pair[bm_unit_id].items():
                factual_acceptance_volumes_by_pair[pair_id] = acceptance_volume
                
        bmu = bm_unit.BMUnit(bm_unit_id, bid_offer_pairs, factual_acceptance_volumes_by_pair, physical_volumes_by_bmu[bm_unit_id])
        bmus[bm_unit_id] = bmu
    
    return bmus

def get_acceptance_volumes_by_bmu_by_pair(grouped_full_settlement_stack):
    acceptance_volumes_by_bmu_by_pair = {}
    for bm_unit_id, boa_data_df in grouped_full_settlement_stack:
        acceptance_volumes_by_pair = {}
        for pair_id, pair_acceptances in boa_data_df.groupby(ct.ColumnHeaders.BID_OFFER_PAIR_ID.value):
            acceptance_volumes_by_pair[pair_id] = pair_acceptances[ct.ColumnHeaders.VOLUME.value].sum()
        acceptance_volumes_by_bmu_by_pair[bm_unit_id] = acceptance_volumes_by_pair
    
    return acceptance_volumes_by_bmu_by_pair
    
def get_marginal_boa(full_ascending_settlement_stack : pd.DataFrame, bid_or_offer_stack):
    ordered_unflagged_actions = full_ascending_settlement_stack[full_ascending_settlement_stack[ct.ColumnHeaders.SO_FLAG.value] == False]
    if (bid_or_offer_stack[ct.ColumnHeaders.PAIR_ID.value] > 0).any():
        ordered_unflagged_offers = ordered_unflagged_actions[ordered_unflagged_actions[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value] > 0]
        if ordered_unflagged_offers.empty:
            marginal_boa = boa.Boa(0, 1, 0)
            return marginal_boa
        marginal_plant_row = get_marginal_plant_offer_row(ordered_unflagged_offers)
        if marginal_plant_row is None:
            marginal_boa = boa.Boa(0, 1, 0)
            return marginal_boa
    else:
        ordered_unflagged_bids = ordered_unflagged_actions[ordered_unflagged_actions[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value] < 0]
        if ordered_unflagged_bids.empty:
            marginal_boa = boa.Boa(0, -1, 0)
            return marginal_boa
        marginal_plant_row = get_marginal_plant_bid_row(ordered_unflagged_bids)
        if marginal_plant_row is None:
            marginal_boa = boa.Boa(0, -1, 0)
            return marginal_boa
    
    marginal_plant_id = marginal_plant_row[ct.ColumnHeaders.ID.value]
    marginal_plant_bid_offer_pair_id = marginal_plant_row[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value]
    marginal_plant_accepted_price = marginal_plant_row[ct.ColumnHeaders.ORIGINAL_PRICE.value]
    marginal_boa = boa.Boa(marginal_plant_id, marginal_plant_bid_offer_pair_id, marginal_plant_accepted_price)
    
    return marginal_boa

def get_marginal_plant_offer_row(ordered_unflagged_offers : pd.DataFrame):
    for i in range(len(ordered_unflagged_offers) - 1, -1, -1):
        row = ordered_unflagged_offers.iloc[i]
        plant_id = row[ct.ColumnHeaders.ID.value]
        plant_offer_id = row[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value]
        dmat_adjusted_volume = row[ct.ColumnHeaders.DMAT_ADJUSTED_VOLUME.value]
        if plant_id is not None and plant_offer_id is not None and dmat_adjusted_volume > 0:
            return row
    
    return None

def get_marginal_plant_bid_row(ordered_unflagged_bids : pd.DataFrame):
    for i in range(len(ordered_unflagged_bids)):
        row = ordered_unflagged_bids.iloc[i]
        plant_id = row[ct.ColumnHeaders.ID.value]
        plant_bid_id = row[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value]
        dmat_adjusted_volume = row[ct.ColumnHeaders.DMAT_ADJUSTED_VOLUME.value]
        if plant_id is not None and plant_bid_id is not None and dmat_adjusted_volume < 0:
            return row
    
    return None

def get_offer_stack_one_period(bid_offer_data_one_period : pd.DataFrame):
    offer_data = bid_offer_data_one_period[bid_offer_data_one_period[ct.ColumnHeaders.PAIR_ID.value] > 0]
    ordered_offer_data = offer_data.sort_values(by=[ct.ColumnHeaders.OFFER.value, ct.ColumnHeaders.PAIR_ID.value], ascending=[True, True])
    ordered_offer_data.reset_index(drop=True, inplace=True)
    
    return ordered_offer_data

def get_bid_stack_one_period(bid_offer_data_one_period : pd.DataFrame):
    bid_data = bid_offer_data_one_period[bid_offer_data_one_period[ct.ColumnHeaders.PAIR_ID.value] < 0]
    ordered_bid_data = bid_data.sort_values(by=[ct.ColumnHeaders.BID.value, ct.ColumnHeaders.PAIR_ID.value], ascending=[False, False])
    ordered_bid_data.reset_index(drop=True, inplace=True)
    
    return ordered_bid_data

def accept_balancing_actions_until_quota_met(energy_before, energy_after, full_ascending_settlement_stack_one_period : pd.DataFrame, bid_or_offer_stack, bmus : list[bm_unit.BMUnit]):
    energy_target = energy_after - energy_before
    
    marginal_boa = get_marginal_boa(full_ascending_settlement_stack_one_period, bid_or_offer_stack)
    marginal_plant_index = set_marginal_plant_index(marginal_boa, bid_or_offer_stack)
    new_acceptances = add_new_bid_offer_acceptances(marginal_plant_index, bid_or_offer_stack, bmus, energy_target)
    ordered_settlement_stack = get_new_ordered_settlement_stack(new_acceptances, full_ascending_settlement_stack_one_period) 
     
    return ordered_settlement_stack

def set_marginal_plant_index(marginal_boa : boa.Boa, bid_or_offer_stack : pd.DataFrame):
    if marginal_boa.plant_id == 0:
        marginal_plant_index = marginal_boa.plant_id #If all actions are for system balancing, we then start accepting offers in merit order
    else:
        marginal_boa_df = bid_or_offer_stack[
            (bid_or_offer_stack[ct.ColumnHeaders.BM_UNIT.value] == marginal_boa.plant_id) &
            (bid_or_offer_stack[ct.ColumnHeaders.PAIR_ID.value] == marginal_boa.bid_offer_pair_id)
        ]
        if not marginal_boa_df.empty:
            marginal_plant_index = marginal_boa_df.index[0]
        else:
            if bid_or_offer_stack[ct.ColumnHeaders.PAIR_ID.value].iloc[0] > 0:
                higher_offers = bid_or_offer_stack[bid_or_offer_stack[ct.ColumnHeaders.OFFER.value] > marginal_boa.accepted_price]
                diff = higher_offers[ct.ColumnHeaders.OFFER.value] - marginal_boa.accepted_price
                marginal_plant_index = higher_offers[diff == diff.min()].index[0]
            else:
                lower_bids = bid_or_offer_stack[bid_or_offer_stack[ct.ColumnHeaders.BID.value] < marginal_boa.accepted_price]
                diff = marginal_boa.accepted_price - lower_bids[ct.ColumnHeaders.BID.value]
                marginal_plant_index = lower_bids[diff == diff.min()].index[0]
        
    return marginal_plant_index

def add_new_bid_offer_acceptances(marginal_plant_index, bid_or_offer_stack, bmus, energy_target):
    total_additional_volume_accepted = 0
    new_acceptances = []
    
    for i in range(marginal_plant_index, len(bid_or_offer_stack)):
        row = bid_or_offer_stack.iloc[i]
        bm_unit_id = row[ct.ColumnHeaders.BM_UNIT.value]
        if bm_unit_id is None: 
            continue
        pair_id = row[ct.ColumnHeaders.PAIR_ID.value]
        bm_unit = bmus[bm_unit_id]
        remaining_volume_in_pair = bm_unit.remaining_volume_by_pair[pair_id]
        
        if pair_id < 0:
            volume_to_accept = max(remaining_volume_in_pair, energy_target - total_additional_volume_accepted) #Both values will always be negative
            pair_price = bm_unit.bid_offer_pairs_submitted[pair_id][ct.ColumnHeaders.BID.value]
        else:
            volume_to_accept = min(remaining_volume_in_pair, energy_target - total_additional_volume_accepted) #Both values will always be positive
            pair_price = bm_unit.bid_offer_pairs_submitted[pair_id][ct.ColumnHeaders.OFFER.value]
        
        total_additional_volume_accepted += volume_to_accept
        new_acceptance = pd.DataFrame([{ct.ColumnHeaders.ID.value: bm_unit_id, 
                                        ct.ColumnHeaders.BID_OFFER_PAIR_ID.value: pair_id, 
                                        ct.ColumnHeaders.SO_FLAG.value: False,
                                        ct.ColumnHeaders.ORIGINAL_PRICE.value: pair_price, 
                                        ct.ColumnHeaders.VOLUME.value: volume_to_accept}])
        new_acceptances.append(new_acceptance)
        if abs(total_additional_volume_accepted) >= abs(energy_target):
            break
        
    return new_acceptances

def get_new_ordered_settlement_stack(new_acceptances, full_ascending_settlement_stack_one_period):
    non_empty_new_acceptances = [acceptance for acceptance in new_acceptances if acceptance.empty == False]
    new_acceptances_df = pd.concat(non_empty_new_acceptances, ignore_index=True)
    new_acceptances_df = new_acceptances_df.reindex(columns=full_ascending_settlement_stack_one_period.columns, fill_value=np.nan)
    new_settlement_stack = pd.concat([full_ascending_settlement_stack_one_period, new_acceptances_df], ignore_index=True)
    ordered_settlement_stack = new_settlement_stack.sort_values(by=[ct.ColumnHeaders.ORIGINAL_PRICE.value, ct.ColumnHeaders.BID_OFFER_PAIR_ID.value], ascending=[True, True])
    ordered_settlement_stack.reset_index(drop=True, inplace=True)
    
    return ordered_settlement_stack

def remove_offers_until_quota_met(niv_with_npts, niv_without_npts, ordered_settlement_stack_one_period : pd.DataFrame):
    energy_surplus = niv_with_npts - niv_without_npts
    total_offer_volume_removed = 0
    row_indices_to_remove = []
    
    for i in range(len(ordered_settlement_stack_one_period) -1, -1, -1):
        row = ordered_settlement_stack_one_period.iloc[i]
        if row[ct.ColumnHeaders.SO_FLAG.value] == True:
            continue
        
        accepted_offer_volume = ordered_settlement_stack_one_period.at[ordered_settlement_stack_one_period.index[i], ct.ColumnHeaders.VOLUME.value]
        
        volume_to_remove = min(accepted_offer_volume, energy_surplus - total_offer_volume_removed)
        total_offer_volume_removed += volume_to_remove
        
        if total_offer_volume_removed >= energy_surplus:
            ordered_settlement_stack_one_period.at[ordered_settlement_stack_one_period.index[i], ct.ColumnHeaders.VOLUME.value] -= volume_to_remove
            break
        else:
            row_indices_to_remove.append(ordered_settlement_stack_one_period.index[i])
        
    new_settlement_stack = ordered_settlement_stack_one_period.drop(index=row_indices_to_remove)
    
    return new_settlement_stack, total_offer_volume_removed

def remove_bids_until_quota_met(niv_with_npts, niv_without_npts, ordered_settlement_stack_one_period : pd.DataFrame):
    energy_deficit = niv_with_npts - niv_without_npts
    total_bid_volume_removed = 0
    row_indices_to_remove = []
    
    for i in range(len(ordered_settlement_stack_one_period)):
        row = ordered_settlement_stack_one_period.iloc[i]
        if row[ct.ColumnHeaders.SO_FLAG.value] == True:
            continue
        
        accepted_bid_volume = ordered_settlement_stack_one_period.at[ordered_settlement_stack_one_period.index[i], ct.ColumnHeaders.VOLUME.value]
        
        volume_to_remove = max(accepted_bid_volume, energy_deficit - total_bid_volume_removed) #Both values will always be negative
        total_bid_volume_removed += volume_to_remove
        
        if total_bid_volume_removed <= energy_deficit:
            ordered_settlement_stack_one_period.at[ordered_settlement_stack_one_period.index[i], ct.ColumnHeaders.VOLUME.value] -= volume_to_remove
            break
        else:
            row_indices_to_remove.append(ordered_settlement_stack_one_period.index[i])
        
    new_settlement_stack = ordered_settlement_stack_one_period.drop(row_indices_to_remove, axis=0)

    return new_settlement_stack, total_bid_volume_removed
import math

import pandas as pd

def get_new_system_prices_by_date_and_period(
    new_settlement_stacks_by_date_and_period: dict,
    ancillary_price_data: pd.DataFrame, 
    tlm_by_bmu: dict, 
    system_imbalance_with_and_without_npts_by_date_and_period: pd.DataFrame
) -> pd.DataFrame:
    system_prices = []
    for settlement_date_and_period, new_settlement_stack in new_settlement_stacks_by_date_and_period.items():
        price_data = ancillary_price_data[ancillary_price_data[ct.ColumnHeaders.DATE_PERIOD_PRIMARY_KEY.value] == settlement_date_and_period]
        market_index_price = price_data[ct.ColumnHeaders.VWAP_MIDP.value].values[0]
        niv_without_npts = system_imbalance_with_and_without_npts_by_date_and_period[system_imbalance_with_and_without_npts_by_date_and_period[ct.ColumnHeaders.DATE_PERIOD_PRIMARY_KEY.value] == settlement_date_and_period][ct.ColumnHeaders.NIV_WITHOUT_NPTS.value].values[0]
        price_adjustment_column_header = ct.ColumnHeaders.BPA.value if niv_without_npts > 0 else ct.ColumnHeaders.SPA.value
        price_adjustment = pd.to_numeric(price_data[price_adjustment_column_header].fillna(0), errors='coerce').values[0]
        new_system_price = get_new_system_price(new_settlement_stack, price_adjustment, market_index_price, tlm_by_bmu, niv_without_npts)
        system_prices.append((settlement_date_and_period, new_system_price))
    
    new_system_prices_df = pd.DataFrame(system_prices, columns=[ct.ColumnHeaders.DATE_PERIOD_PRIMARY_KEY.value, ct.ColumnHeaders.RECALCULATED_SYSTEM_PRICE.value])
   
    return new_system_prices_df

def get_new_system_price(
    settlement_stack: pd.DataFrame, 
    price_adjustment: float, 
    market_index_price: float, 
    tlm_by_bmu: dict, 
    niv_without_npts: float
) -> float:
    if settlement_stack.empty:
        return market_index_price
    buy_ranked_set, sell_ranked_set = get_ranked_sets(settlement_stack, niv_without_npts)
    dmat_adjusted_buy_set, dmat_adjusted_sell_set = perform_de_minimis_tagging(buy_ranked_set, sell_ranked_set, niv_without_npts)
    arbitrage_adjusted_buy_set, arbitrage_adjusted_sell_set = perform_arbitrage_tagging(dmat_adjusted_buy_set, dmat_adjusted_sell_set)
    classified_buy_set, classified_sell_set = perform_classification(arbitrage_adjusted_buy_set, arbitrage_adjusted_sell_set)
    niv_adjusted_buy_set, niv_adjusted_sell_set = perform_niv_tagging(classified_buy_set, classified_sell_set, niv_without_npts)
    ranked_set_for_calculation = niv_adjusted_buy_set if niv_without_npts > 0 else niv_adjusted_sell_set
    if ranked_set_for_calculation.empty:
        return market_index_price
    niv_adjusted_actions_only = ranked_set_for_calculation[ranked_set_for_calculation['niv_adjusted_volume'] != 0]
    if niv_adjusted_actions_only['niv_adjusted_voklume'].sum() == 0:
        return market_index_price
    ranked_set_with_final_prices = replace_prices_for_second_stage_flagged_actions(niv_adjusted_actions_only, market_index_price, niv_without_npts)
    par_tagged_ranked_set = perform_par_tagging(ranked_set_with_final_prices, niv_without_npts)
    tlm_adjusted_ranked_set = get_tlm_adjusted_ranked_set(par_tagged_ranked_set, tlm_by_bmu)
    imbalance_price = perform_final_imbalance_price_calculation(tlm_adjusted_ranked_set, price_adjustment)
    
    return imbalance_price
    
def get_ranked_sets(
    new_settlement_stack: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    buy_ranked_set = new_settlement_stack[new_settlement_stack['volume'] >= 0].sort_values(
        by='original_price', ascending=True).reset_index(drop=True) #System buying to increase generation
    sell_ranked_set = new_settlement_stack[new_settlement_stack['volume'] < 0].sort_values(
        by='original_price', ascending=False).reset_index(drop=True)
    
    return buy_ranked_set, sell_ranked_set

def perform_de_minimis_tagging(
    buy_ranked_set: pd.DataFrame,
    sell_ranked_set : pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    buy_volume_by_bmu_by_pair_id = get_total_volume_by_bmu_by_pair(buy_ranked_set)
    sell_volume_by_bmu_by_pair_id = get_total_volume_by_bmu_by_pair(sell_ranked_set)
    
    for buy_index, buy_row in buy_ranked_set.iterrows():
        bmu = buy_row['ID']
        bid_offer_pair_id = buy_row['bid_offer_pair_id']
        if pd.isna(bid_offer_pair_id):
            bid_offer_pair_id = 0
        original_volume = buy_row['volume']
        if (bmu, bid_offer_pair_id) in buy_volume_by_bmu_by_pair_id:
            total_buy_volume = buy_volume_by_bmu_by_pair_id[(bmu, bid_offer_pair_id)]    
        else:
            total_buy_volume = original_volume
        dmat_volume = original_volume
        if original_volume < 0.1 and total_buy_volume < 0.1:
            dmat_volume = 0
        buy_ranked_set.at[buy_index, 'dmat_adjusted_volume'] = dmat_volume
        
    for sell_index, sell_row in sell_ranked_set.iterrows():
        bmu = sell_row['id']
        bid_offer_pair_id = sell_row['bid_offer_pair_id']
        if pd.isna(bid_offer_pair_id):
            bid_offer_pair_id = 0
        original_volume = sell_row['volume']
        if (bmu, bid_offer_pair_id) in sell_volume_by_bmu_by_pair_id:
            total_sell_volume = sell_volume_by_bmu_by_pair_id[(bmu, bid_offer_pair_id)]
        else:
            total_sell_volume = original_volume
        dmat_volume = original_volume
        if original_volume > -0.1 and total_sell_volume > -0.1:
            dmat_volume = 0
        sell_ranked_set.at[sell_index, 'dmat_adjusted_volume'] = dmat_volume
    
    return buy_ranked_set, sell_ranked_set

def perform_arbitrage_tagging(
    dmat_adjusted_buy_set: pd.DataFrame, 
    dmat_adjusted_sell_set: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dmat_adjusted_buy_set.loc[:, 'arbitrage_adjusted_volume'] = dmat_adjusted_buy_set['dmat_adjusted_volume']
    dmat_adjusted_sell_set.loc[:, 'arbitrage_adjusted_volume'] = dmat_adjusted_sell_set['dmat_adjusted_volume']
    for sell_index, sell_row in dmat_adjusted_sell_set.iterrows():
        sell_price = sell_row['original_price']
        sell_volume = dmat_adjusted_sell_set.at[sell_index, 'arbitrage_adjusted_volume']
        if sell_volume == 0:
            continue
        cheaper_buy_actions = dmat_adjusted_buy_set[dmat_adjusted_buy_set['original_price'] <= sell_price]
        
        for buy_index, buy_row in cheaper_buy_actions.iterrows():
            if sell_volume == 0:
                break
            sell_value = -sell_price * sell_volume #Change to positive value for ease of calculation, see section 13.2.c of https://bscdocs.elexon.co.uk/bsc/bsc-section-t-settlement-and-trading-charges#annex-t-1
            buy_price = buy_row['original_price']
            buy_volume = dmat_adjusted_buy_set.at[buy_index, 'arbitrage_adjusted_volume']
            if buy_volume == 0:
                continue
            buy_value = buy_price * buy_volume
            if buy_value >= sell_value:
                buy_volume_to_remove = min(-sell_volume, buy_volume)
                dmat_adjusted_buy_set.at[buy_index, 'arbitrage_adjusted_volume'] -= buy_volume_to_remove
                dmat_adjusted_sell_set.at[sell_index, 'arbitrage_adjusted_volume'] += buy_volume_to_remove
                break
            else:
                sell_volume_to_remove = min(-sell_volume, buy_volume)
                dmat_adjusted_buy_set.at[buy_index, 'arbitrage_adjusted_volume'] -= sell_volume_to_remove
                dmat_adjusted_sell_set.at[sell_index, 'arbitrage_adjusted_volume'] += sell_volume_to_remove
                sell_volume += sell_volume_to_remove
        
    arbitrage_adjusted_buy_set = dmat_adjusted_buy_set.copy()
    arbitrage_adjusted_sell_set = dmat_adjusted_sell_set.copy()
    
    return arbitrage_adjusted_buy_set, arbitrage_adjusted_sell_set

def perform_classification(arbitrage_adjusted_buy_set : pd.DataFrame, arbitrage_adjusted_sell_set : pd.DataFrame):
    first_stage_unflagged_buy_actions = arbitrage_adjusted_buy_set[
        (arbitrage_adjusted_buy_set[ct.ColumnHeaders.SO_FLAG.value] == False) &
        (arbitrage_adjusted_buy_set[ct.ColumnHeaders.CADL_FLAG.value] != True) &
        (arbitrage_adjusted_buy_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value] != 0)
    ]
    first_stage_unflagged_sell_actions = arbitrage_adjusted_sell_set[
        (arbitrage_adjusted_sell_set[ct.ColumnHeaders.SO_FLAG.value] == False) &
        (arbitrage_adjusted_sell_set[ct.ColumnHeaders.CADL_FLAG.value] != True) &
        (arbitrage_adjusted_sell_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value] != 0)
    ]
    if first_stage_unflagged_buy_actions.empty:
        if not arbitrage_adjusted_buy_set.empty:
            arbitrage_adjusted_buy_set.loc[:, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = True
    else:
        most_expensive_unflagged_buy_action = first_stage_unflagged_buy_actions.iloc[-1][ct.ColumnHeaders.ORIGINAL_PRICE.value]
        for index, row in arbitrage_adjusted_buy_set.iterrows():
            if row[ct.ColumnHeaders.ORIGINAL_PRICE.value] is None or math.isnan(row[ct.ColumnHeaders.ORIGINAL_PRICE.value]):
                continue #8.4: any null priced action shall not become unflagged
            elif row[ct.ColumnHeaders.ORIGINAL_PRICE.value] > most_expensive_unflagged_buy_action:
                arbitrage_adjusted_buy_set.at[index, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = True
            else:
                arbitrage_adjusted_buy_set.at[index, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = False
    
    if first_stage_unflagged_sell_actions.empty:
        if not arbitrage_adjusted_sell_set.empty:
            arbitrage_adjusted_sell_set.loc[:, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = True
    else:
        most_expensive_unflagged_sell_action = first_stage_unflagged_sell_actions.iloc[-1][ct.ColumnHeaders.ORIGINAL_PRICE.value]
        for index, row in arbitrage_adjusted_sell_set.iterrows():
            if row[ct.ColumnHeaders.ORIGINAL_PRICE.value] is None or math.isnan(row[ct.ColumnHeaders.ORIGINAL_PRICE.value]):
                continue #8.4: any null priced action shall not become unflagged
            elif row[ct.ColumnHeaders.ORIGINAL_PRICE.value] < most_expensive_unflagged_sell_action:
                arbitrage_adjusted_sell_set.at[index, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = True
            else:
                arbitrage_adjusted_sell_set.at[index, ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] = False
    
    arbitrage_adjusted_buy_set.loc[:, ct.ColumnHeaders.REPRICED_INDICATOR.value] = False
    arbitrage_adjusted_sell_set.loc[:, ct.ColumnHeaders.REPRICED_INDICATOR.value] = False
             
    classified_buy_set = arbitrage_adjusted_buy_set.copy()
    classified_sell_set = arbitrage_adjusted_sell_set.copy()
    
    return classified_buy_set, classified_sell_set

def perform_niv_tagging(classified_buy_ranked_set : pd.DataFrame, classified_sell_ranked_set : pd.DataFrame, niv_without_npts):
    classified_buy_ranked_set.loc[:, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = classified_buy_ranked_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value]
    classified_sell_ranked_set.loc[:, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = classified_sell_ranked_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value]
    total_buy_volume = classified_buy_ranked_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value].sum()
    total_sell_volume = classified_sell_ranked_set[ct.ColumnHeaders.ARBITRAGE_ADJUSTED_VOLUME.value].sum()
    net_imbalance = total_buy_volume + total_sell_volume
    if net_imbalance > 0:
        for buy_index, buy_row in classified_buy_ranked_set.iloc[::-1].iterrows():
            for sell_index, sell_row in classified_sell_ranked_set.iterrows():
                buy_volume = classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
                sell_volume = classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
                if sell_volume == 0:
                    continue
                if sell_volume + buy_volume <= 0:
                    classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] += buy_volume
                    classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = 0
                    break
                else:
                    classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = 0
                    classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] += sell_volume
    else:
        for sell_index, sell_row in classified_sell_ranked_set.iloc[::-1].iterrows():
            for buy_index, buy_row in classified_buy_ranked_set.iterrows():
                buy_volume = classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
                sell_volume = classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
                if buy_volume == 0:
                    continue
                if sell_volume + buy_volume >= 0:
                    classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] += sell_volume
                    classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = 0
                    break
                else:
                    classified_buy_ranked_set.at[buy_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] = 0
                    classified_sell_ranked_set.at[sell_index, ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value] += buy_volume
                    
    niv_adjusted_buy_ranked_set = classified_buy_ranked_set.copy()
    niv_adjusted_sell_ranked_set = classified_sell_ranked_set.copy()
    
    return niv_adjusted_buy_ranked_set, niv_adjusted_sell_ranked_set

def replace_prices_for_second_stage_flagged_actions(niv_adjusted_ranked_set : pd.DataFrame, market_index_price, niv_without_npts):
    niv_adjusted_ranked_set.loc[:, ct.ColumnHeaders.FINAL_PRICE.value] = niv_adjusted_ranked_set[ct.ColumnHeaders.ORIGINAL_PRICE.value]
    if not niv_adjusted_ranked_set[niv_adjusted_ranked_set[ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] == True].empty:
        second_stage_unflagged_actions = niv_adjusted_ranked_set[niv_adjusted_ranked_set[ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] == False]
        if second_stage_unflagged_actions.empty:
            replacement_price = market_index_price
        else:
            replacement_price = calculate_replacement_price(second_stage_unflagged_actions)   
        
        niv_adjusted_ranked_set.loc[niv_adjusted_ranked_set[ct.ColumnHeaders.SECOND_STAGE_FLAGGED.value] == True, ct.ColumnHeaders.REPRICED_INDICATOR.value] = True
        niv_adjusted_ranked_set.loc[niv_adjusted_ranked_set[ct.ColumnHeaders.REPRICED_INDICATOR.value] == True, ct.ColumnHeaders.FINAL_PRICE.value] = replacement_price
        sort_ascending = niv_without_npts > 0
        niv_adjusted_ranked_set = niv_adjusted_ranked_set.sort_values(by=ct.ColumnHeaders.FINAL_PRICE.value, ascending=sort_ascending)
        niv_adjusted_ranked_set.reset_index(drop=True, inplace=True)

    ranked_set_with_final_prices = niv_adjusted_ranked_set.copy()
    
    return ranked_set_with_final_prices

def perform_par_tagging(ranked_set_with_final_prices : pd.DataFrame, niv_without_npts):
    total_par_volume = 0
    ranked_set_with_final_prices.loc[:, ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] = float(0)
    if niv_without_npts > 0:
        for index, row in ranked_set_with_final_prices.iloc[::-1].iterrows():
            volume = row[ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
            if volume + total_par_volume > ct.SystemDefaults.PAR_VOLUME:
                ranked_set_with_final_prices.at[index, ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] = ct.SystemDefaults.PAR_VOLUME - total_par_volume
                break
            else:
                ranked_set_with_final_prices.at[index, ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] = volume
                total_par_volume += volume
    else:
        for index, row in ranked_set_with_final_prices.iloc[::-1].iterrows():
            volume = row[ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value]
            if volume + total_par_volume < -ct.SystemDefaults.PAR_VOLUME:
                ranked_set_with_final_prices.at[index, ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] = -ct.SystemDefaults.PAR_VOLUME - total_par_volume
                break
            else:
                ranked_set_with_final_prices.at[index, ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] = volume
                total_par_volume += volume
    
    par_tagged_ranked_set = ranked_set_with_final_prices[ranked_set_with_final_prices[ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] != 0]
    
    return par_tagged_ranked_set

def get_tlm_adjusted_ranked_set(par_tagged_ranked_set : pd.DataFrame, tlm_by_bmu : dict):
    par_tagged_ranked_set.loc[:, ct.ColumnHeaders.TLM_ADJUSTED_COST.value] = float(0)
    for index, row in par_tagged_ranked_set.iterrows():
        bmu_id = row[ct.ColumnHeaders.ID.value]
        tlm = tlm_by_bmu[bmu_id] if bmu_id in tlm_by_bmu else 1
        par_tagged_ranked_set.at[index, ct.ColumnHeaders.TLM_ADJUSTED_VOLUME.value] = row[ct.ColumnHeaders.PAR_ADJUSTED_VOLUME.value] * tlm
        par_tagged_ranked_set.at[index, ct.ColumnHeaders.TLM_ADJUSTED_COST.value] = par_tagged_ranked_set.at[index, ct.ColumnHeaders.TLM_ADJUSTED_VOLUME.value] * row[ct.ColumnHeaders.FINAL_PRICE.value]
    
    tlm_adjusted_ranked_set = par_tagged_ranked_set.copy()
    
    return tlm_adjusted_ranked_set

def perform_final_imbalance_price_calculation(tlm_adjusted_ranked_set : pd.DataFrame, price_adjustment):
    total_tlm_adjusted_volume = tlm_adjusted_ranked_set[ct.ColumnHeaders.TLM_ADJUSTED_VOLUME.value].sum()
    total_tlm_adjusted_cost = tlm_adjusted_ranked_set[ct.ColumnHeaders.TLM_ADJUSTED_COST.value].sum()
    imbalance_price = total_tlm_adjusted_cost / total_tlm_adjusted_volume + price_adjustment
    
    return imbalance_price

def get_total_volume_by_bmu_by_pair(ranked_set : pd.DataFrame):
    total_volume_by_bmu = {}
    for bmu, boas in ranked_set.groupby(ct.ColumnHeaders.ID.value):
        for acceptance_index, acceptance_row in boas.iterrows():
            pair_id = acceptance_row[ct.ColumnHeaders.BID_OFFER_PAIR_ID.value]
            if pd.isna(pair_id):
                pair_id = 0
            total_volume = acceptance_row[ct.ColumnHeaders.VOLUME.value]
            if (bmu, pair_id) in total_volume_by_bmu:
                total_volume_by_bmu[(bmu, pair_id)] += total_volume
            else:
                total_volume_by_bmu[(bmu, pair_id)] = total_volume
    
    return total_volume_by_bmu

def calculate_replacement_price(niv_adjusted_ranked_set_second_stage_unflagged : pd.DataFrame):
    total_rpar_volume = 0
    volumes_for_replacement_price = []
    prices_for_replacement_price = []
    for index, row in niv_adjusted_ranked_set_second_stage_unflagged.iloc[::-1].iterrows():
        volume = abs(row[ct.ColumnHeaders.NIV_ADJUSTED_VOLUME.value])
        price = row[ct.ColumnHeaders.ORIGINAL_PRICE.value]
        if volume + total_rpar_volume > ct.SystemDefaults.RPAR_VOLUME:
            volumes_for_replacement_price.append(ct.SystemDefaults.RPAR_VOLUME - total_rpar_volume)
            prices_for_replacement_price.append(price)
            break
        else:
            volumes_for_replacement_price.append(volume)
            prices_for_replacement_price.append(price)
            total_rpar_volume += volume
    
    replacement_price = sum([volume * price for volume, price in zip(volumes_for_replacement_price, prices_for_replacement_price)]) / sum(volumes_for_replacement_price)
    
    return replacement_price
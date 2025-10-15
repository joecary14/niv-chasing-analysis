import pandas as pd

def calculate_imbalance_volume_by_unit(
    raw_bm_103_data_by_date: list[pd.DataFrame],
    imbalance_price_data: pd.DataFrame,
    exchange_rate_data: pd.DataFrame
) -> pd.DataFrame:
    data_rows = []
    imbalance_price_data['StartTime'] = pd.to_datetime(imbalance_price_data['StartTime'], format='%Y-%m-%dT%H:%M:%S', utc=True)
    imbalance_price_data = imbalance_price_data[imbalance_price_data['FromCurrency'] == 'E']
    imbalance_price_data = imbalance_price_data.merge(
        exchange_rate_data[['settlement_date', 'ExchangeRate']], 
        on='settlement_date', 
        how='left'
    )
    imbalance_price_data['ImbalanceSettlementPriceGbp'] = imbalance_price_data['ImbalanceSettlementPrice'] * imbalance_price_data['ExchangeRate']
    
    for bm_103_df in raw_bm_103_data_by_date:
        if bm_103_df.empty:
            continue
        cimb_au_data = bm_103_df[
            (bm_103_df['determinant'] == 'CIMB') &
            (bm_103_df['resource'].str.startswith('AU'))
        ]
        cimb_au_data['datetime'] = pd.to_datetime(cimb_au_data['datetime']).dt.tz_convert('UTC')
        for start_time, au_imbalance_data_one_start_time in cimb_au_data.groupby('datetime'):
            matching_price_data = imbalance_price_data[imbalance_price_data['StartTime'] == start_time]
            if matching_price_data.empty:
                continue
            imbalance_price_eur = matching_price_data.iloc[0]['ImbalanceSettlementPrice']
            imbalance_price_gbp = matching_price_data.iloc[0]['ImbalanceSettlementPriceGbp']
            niv = matching_price_data.iloc[0]['NetImbalanceVolume']
            unique_au_cimb_data = au_imbalance_data_one_start_time.drop_duplicates(subset=['resource'])
            unique_au_cimb_data['applicable_imbalance_price'] = unique_au_cimb_data['unit'].apply(
                lambda x: imbalance_price_eur if x == 'EUR' else imbalance_price_gbp if x == 'GBP' else None
            )
            unique_au_cimb_data['imbalance_volume'] = unique_au_cimb_data.apply(
                lambda row: row['amount'] / row['applicable_imbalance_price'] if row['applicable_imbalance_price'] and row['applicable_imbalance_price'] != 0 else None,
                axis=1
            )
            aggregate_au_imbalance = au_imbalance_data_one_start_time['imbalance_volume'].sum()
            counterfactual_niv = niv + aggregate_au_imbalance
            
            data_rows.append({
                'datetime': start_time,
                'imbalance_price_eur': imbalance_price_eur,
                'imbalance_price_gbp': imbalance_price_gbp,
                'net_imbalance_volume': niv,
                'aggregate_au_imbalance_volume': aggregate_au_imbalance,
                'counterfactual_niv': counterfactual_niv
            })
    
    result_df = pd.DataFrame(data_rows)
    
    return result_df
            
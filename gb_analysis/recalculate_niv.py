import datetime
import calendar
import pandas as pd
from data_collection import elexon_interaction
from elexonpy.api_client import ApiClient
from ancillary_files import excel_interaction
from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day

async def recalculate_niv(
    year: int,
    month: int,
    mr1b_filepath: str,
    bsc_roles_to_npt_mapping: str
) -> pd.DataFrame:
    api_client = ApiClient()
    data = []   
    month_start_date = datetime.date(year, month, 1).strftime('%Y-%m-%d')
    _, last_day = calendar.monthrange(year, month)
    month_end_date = datetime.date(year, month, last_day).strftime('%Y-%m-%d')
    settlement_dates_and_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(
        month_start_date, month_end_date, True)
    niv_data = await elexon_interaction.get_niv_data(settlement_dates_and_periods_per_day, api_client)
    npt_imbalances = get_npt_imbalance_data(mr1b_filepath, bsc_roles_to_npt_mapping)
    niv_data.drop(columns=['start_time'], inplace=True)
    
    outturn_system_length = standardize_merge_columns(outturn_system_length)
    npt_imbalances = standardize_merge_columns(npt_imbalances)
    
    combined_data = outturn_system_length.merge(
        npt_imbalances, 
        on=['settlement_date', 'settlement_period'], 
        how='outer', 
        suffixes=('', '_niv')
    )
    combined_data['counterfactual_niv'] = combined_data['net_imbalance_volume'] + combined_data['npt_total_imbalance']
    data.append(combined_data)
    
    full_dataset = pd.concat(data, ignore_index=True)

    return full_dataset

def get_bsc_roles_to_npt_mapping(
    bsc_roles_filepath: str,
    strict_npt_mapping: bool
) -> dict[str, bool]:
    bsc_roles_df = pd.read_excel(bsc_roles_filepath)
    bsc_id_to_npt_mapping = {}
    for _, row in bsc_roles_df.iterrows():
        is_npt = row['TN']
        is_strict_npt = is_npt and not row['TS'] and not row['TG']
        bsc_id_to_npt_mapping[row['BSC_ID']] = is_strict_npt if strict_npt_mapping else is_npt
        
    return bsc_id_to_npt_mapping

def get_npt_imbalance_data(
    mr1b_filepath: str,
    bsc_roles_to_npt_mapping: dict
) -> pd.DataFrame:
    mr1b_df = pd.read_excel(mr1b_filepath)
    mr1b_df = mr1b_df.map(lambda x: x.strip() if isinstance(x, str) else x)
    mr1b_df_npts_only = mr1b_df[mr1b_df['Party ID'].map(bsc_roles_to_npt_mapping) == True]
    settlement_dates = []
    settlement_periods = []
    energy_imbalances = []
    for settlement_date, group in mr1b_df_npts_only.groupby('Settlement Date'):
        for settlement_period, group_by_period in group.groupby('Settlement Period'):
            energy_imbalance = group_by_period['Energy Imbalance Vol'].sum()
            settlement_dates.append(settlement_date)
            settlement_periods.append(settlement_period)
            energy_imbalances.append(energy_imbalance)
    grouped = mr1b_df_npts_only.groupby(['Settlement Date', 'Settlement Period'])['Energy Imbalance Vol'].sum().reset_index()
    grouped.columns = ['settlement_date', 'settlement_period', 'npt_total_imbalance']
    
    return grouped

def standardize_merge_columns(
    df: pd.DataFrame
):
    df = df.copy()
    df['settlement_date'] = pd.to_datetime(df['settlement_date']).dt.date.astype(str)
    df['settlement_period'] = df['settlement_period'].astype(int)
    return df
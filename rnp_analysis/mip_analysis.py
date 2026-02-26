import asyncio
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day
from ancillary_files.excel_interaction import create_filepath
from data_collection.elexon_interaction import get_niv_data
from data_processing.price_data_processing import get_market_index_price_data
from elexonpy.api_client import ApiClient

IMBALANCE_VOUME_DATA_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/RNP/Analysis/Imbalance Volume Data.xlsx'
COMBINED_DATA_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/RNP/Analysis/Imbalance Volume Data_with_mip_system_sell_price_2021-01-01_to_2024-12-31.xlsx'




def _normalise_settlement_keys(data: pd.DataFrame) -> pd.DataFrame:
    normalised_data = data.copy()
    normalised_data['settlement_date'] = pd.to_datetime(
        normalised_data['settlement_date'],
        errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    normalised_data['settlement_period'] = pd.to_numeric(
        normalised_data['settlement_period'],
        errors='coerce'
    )
    normalised_data = normalised_data.dropna(subset=['settlement_date', 'settlement_period'])
    normalised_data['settlement_period'] = normalised_data['settlement_period'].astype(int)

    return normalised_data

async def get_mip_and_system_sell_price_data(
    start_date: str,
    end_date: str,
    api_client: ApiClient | None = None
) -> pd.DataFrame:
    settlement_dates_and_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(start_date, end_date)
    if not settlement_dates_and_periods_per_day:
        return pd.DataFrame(columns=['settlement_date', 'settlement_period', 'vwap_midp', 'system_sell_price'])

    client = api_client or ApiClient()

    market_index_task = get_market_index_price_data(settlement_dates_and_periods_per_day, client)
    niv_task = get_niv_data(settlement_dates_and_periods_per_day, client)
    market_index_data, niv_data = await asyncio.gather(market_index_task, niv_task)
    market_index_data = _normalise_settlement_keys(market_index_data)
    niv_data = _normalise_settlement_keys(niv_data)

    system_sell_price_data = niv_data[['settlement_date', 'settlement_period', 'system_sell_price']].drop_duplicates()

    combined_data = market_index_data.merge(
        system_sell_price_data,
        on=['settlement_date', 'settlement_period'],
        how='outer'
    )
    combined_data = combined_data.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)

    return combined_data


def _normalise_imbalance_volume_columns(imbalance_volume_data: pd.DataFrame) -> pd.DataFrame:
    normalised_data = imbalance_volume_data.copy()

    canonical_by_normalised_name = {
        column_name.strip().lower().replace(' ', '_'): column_name
        for column_name in normalised_data.columns
    }

    settlement_date_column = None
    for candidate in ['settlement_date', 'settlementdate', 'date', 'settlement_date_utc']:
        if candidate in canonical_by_normalised_name:
            settlement_date_column = canonical_by_normalised_name[candidate]
            break

    settlement_period_column = None
    for candidate in ['settlement_period', 'settlementperiod', 'period', 'sp']:
        if candidate in canonical_by_normalised_name:
            settlement_period_column = canonical_by_normalised_name[candidate]
            break

    if settlement_date_column is None or settlement_period_column is None:
        raise ValueError(
            'Input Excel must contain settlement date and settlement period columns. '
            f'Found columns: {list(normalised_data.columns)}'
        )

    normalised_data = normalised_data.rename(
        columns={
            settlement_date_column: 'settlement_date',
            settlement_period_column: 'settlement_period'
        }
    )
    normalised_data = _normalise_settlement_keys(normalised_data)

    return normalised_data


async def combine_mip_system_sell_price_with_imbalance_volume(
    start_date: str | None = None,
    end_date: str | None = None,
    sheet_name: str | int = 0,
    api_client: ApiClient | None = None
) -> str:
    imbalance_volume_data = pd.read_excel(IMBALANCE_VOUME_DATA_FILEPATH, sheet_name=sheet_name)
    imbalance_volume_data = _normalise_imbalance_volume_columns(imbalance_volume_data)

    if imbalance_volume_data.empty:
        raise ValueError('No valid imbalance volume rows were found in the provided Excel file.')

    effective_start_date = start_date or imbalance_volume_data['settlement_date'].min()
    effective_end_date = end_date or imbalance_volume_data['settlement_date'].max()

    mip_and_system_sell_price_data = await get_mip_and_system_sell_price_data(
        effective_start_date,
        effective_end_date,
        api_client=api_client
    )
    mip_and_system_sell_price_data = _normalise_settlement_keys(mip_and_system_sell_price_data)

    combined_data = imbalance_volume_data.merge(
        mip_and_system_sell_price_data,
        on=['settlement_date', 'settlement_period'],
        how='left'
    )
    combined_data = combined_data.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)

    input_path = Path(IMBALANCE_VOUME_DATA_FILEPATH)
    output_directory = str(input_path.parent)
    output_filename = (
        f"{input_path.stem}_with_mip_system_sell_price_{effective_start_date}_to_{effective_end_date}.xlsx"
    )
    output_filepath = create_filepath(output_directory, output_filename)
    combined_data.to_excel(output_filepath, index=False)

    return output_filepath


def _ecdf(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if values.size == 0:
        return np.array([]), np.array([])
    x_values = np.sort(values)
    y_values = np.arange(1, x_values.size + 1) / x_values.size

    return x_values, y_values


def plot_benefit_ecdfs_by_default_status(
    combined_data_filepath: str = COMBINED_DATA_FILEPATH,
    scenario_column: str = 'AMV',
    sheet_name: str | int = 0,
    x_min: float = -1000,
    x_max: float = 1000,
    price_equality_tolerance: float = 1e-9
) -> str:
    combined_data = pd.read_excel(combined_data_filepath, sheet_name=sheet_name)

    required_columns = ['Factual', 'AMV', 'ZMV', 'vwap_midp', 'system_sell_price']
    missing_columns = [column for column in required_columns if column not in combined_data.columns]
    if missing_columns:
        raise ValueError(
            f'Missing required columns: {missing_columns}. Available columns: {list(combined_data.columns)}'
        )

    scenario_column_upper = scenario_column.upper()
    if scenario_column_upper == 'AMZ':
        scenario_column_upper = 'AMV'
    if scenario_column_upper not in ['AMV', 'ZMV']:
        raise ValueError("scenario_column must be one of 'AMV', 'AMZ', or 'ZMV'.")

    for column in required_columns:
        combined_data[column] = pd.to_numeric(combined_data[column], errors='coerce')

    combined_data['amv_benefit'] = combined_data['AMV'].abs() - combined_data['Factual'].abs()
    combined_data['zmv_benefit'] = combined_data['ZMV'].abs() - combined_data['Factual'].abs()
    combined_data['system_sell_price_defaults_to_vwap_midp'] = np.isclose(
        combined_data['system_sell_price'],
        combined_data['vwap_midp'],
        atol=price_equality_tolerance,
        rtol=0
    )

    benefit_column = 'amv_benefit' if scenario_column_upper == 'AMV' else 'zmv_benefit'
    plot_data = combined_data[[benefit_column, 'system_sell_price_defaults_to_vwap_midp']].dropna()

    all_periods_benefit = plot_data[benefit_column].to_numpy()
    defaulted_periods_benefit = plot_data.loc[
        plot_data['system_sell_price_defaults_to_vwap_midp'],
        benefit_column
    ].to_numpy()
    non_defaulted_periods_benefit = plot_data.loc[
        ~plot_data['system_sell_price_defaults_to_vwap_midp'],
        benefit_column
    ].to_numpy()

    def restrict_range(values: np.ndarray) -> np.ndarray:
        return values[(values >= x_min) & (values <= x_max)]

    all_periods_benefit = restrict_range(all_periods_benefit)
    defaulted_periods_benefit = restrict_range(defaulted_periods_benefit)
    non_defaulted_periods_benefit = restrict_range(non_defaulted_periods_benefit)

    x_all, y_all = _ecdf(all_periods_benefit)
    x_defaulted, y_defaulted = _ecdf(defaulted_periods_benefit)
    x_non_defaulted, y_non_defaulted = _ecdf(non_defaulted_periods_benefit)

    plt.close('all')
    fig, axis = plt.subplots(figsize=(9, 6))
    if x_all.size > 0:
        axis.step(x_all, y_all, where='post', label='All periods', linewidth=1.5)
    if x_defaulted.size > 0:
        axis.step(x_defaulted, y_defaulted, where='post', label='Periods where vwap_midp = system_sell_price', linewidth=1.5, linestyle='--')
    if x_non_defaulted.size > 0:
        axis.step(x_non_defaulted, y_non_defaulted, where='post', label='Periods where vwap_midp != system_sell_price', linewidth=1.5, linestyle=':')

    axis.set_xlim(x_min, x_max)
    axis.set_ylim(-0.02, 1.02)
    axis.set_xlabel(f'{scenario_column_upper} Benefit (|{scenario_column_upper}| - |Factual|)')
    axis.set_ylabel('Empirical Cumulative Probability')
    axis.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    axis.axvline(0, color='black', linestyle='--', linewidth=1)
    axis.legend(frameon=False)
    plt.tight_layout()

    combined_path = Path(combined_data_filepath)
    output_directory = str(combined_path.parent)
    output_filename = f'{combined_path.stem}_{scenario_column_upper.lower()}_benefit_ecdf_by_price_default_status.pdf'
    output_filepath = create_filepath(output_directory, output_filename)
    plt.savefig(output_filepath, format='pdf', bbox_inches='tight')
    plt.close(fig)

    return output_filepath


def plot_amv_and_zmv_benefit_ecdfs_by_default_status(
    combined_data_filepath: str = COMBINED_DATA_FILEPATH,
    sheet_name: str | int = 0,
    x_min: float = -1000,
    x_max: float = 1000,
    price_equality_tolerance: float = 1e-9
) -> dict[str, str]:
    amv_output = plot_benefit_ecdfs_by_default_status(
        combined_data_filepath=combined_data_filepath,
        scenario_column='AMV',
        sheet_name=sheet_name,
        x_min=x_min,
        x_max=x_max,
        price_equality_tolerance=price_equality_tolerance
    )
    zmv_output = plot_benefit_ecdfs_by_default_status(
        combined_data_filepath=combined_data_filepath,
        scenario_column='ZMV',
        sheet_name=sheet_name,
        x_min=x_min,
        x_max=x_max,
        price_equality_tolerance=price_equality_tolerance
    )

    return {
        'AMV': amv_output,
        'ZMV': zmv_output
    }



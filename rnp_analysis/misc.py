from pathlib import Path

import pandas as pd

from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day
from ancillary_files.excel_interaction import create_filepath
from elexonpy.api_client import ApiClient
from gb_analysis.recalculate_niv import (
	get_bsc_id_to_npt_mapping,
	recalculate_niv,
	recalculate_niv_zero_metered_volume,
)


MR1B_2025_ONWARDS_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Data/Elexon/mr1b_since_2025.csv'
IMBALANCE_VOLUME_DATA_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/RNP/Analysis/Imbalance Volume Data.xlsx'
BSC_ROLES_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'


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

	return _normalise_settlement_keys(normalised_data)


def _get_credited_energy_column(columns: pd.Index) -> str:
	if 'Credited Energy Vol' in columns:
		return 'Credited Energy Vol'
	if 'CreditedEnergyVol' in columns:
		return 'CreditedEnergyVol'
	raise ValueError(
		"MR1B data must include either 'Credited Energy Vol' or 'CreditedEnergyVol'."
	)


def _normalise_column_name(column_name: str) -> str:
	return ''.join(character for character in column_name.lower() if character.isalnum())


def _resolve_mr1b_columns(columns: pd.Index) -> dict[str, str]:
	column_lookup = {
		_normalise_column_name(column_name): column_name
		for column_name in columns
	}

	def pick_column(candidates: list[str], label: str) -> str:
		for candidate in candidates:
			if candidate in column_lookup:
				return column_lookup[candidate]
		raise ValueError(
			f"MR1B data is missing '{label}'. Available columns: {list(columns)}"
		)

	return {
		'settlement_date': pick_column(['settlementdate'], 'Settlement Date'),
		'settlement_period': pick_column(['settlementperiod'], 'Settlement Period'),
		'party_id': pick_column(['partyid'], 'Party ID / PartyID'),
		'energy_imbalance_vol': pick_column(['energyimbalancevol'], 'Energy Imbalance Vol / EnergyImbalanceVol'),
		'credited_energy_vol': pick_column(['creditedenergyvol'], 'Credited Energy Vol / CreditedEnergyVol')
	}


def _load_mr1b_rows_for_date_range(
	mr1b_filepath: str,
	start_date: str,
	end_date: str,
	chunksize: int = 250_000
) -> pd.DataFrame:
	start_ts = pd.Timestamp(start_date)
	end_ts = pd.Timestamp(end_date)
	filtered_chunks: list[pd.DataFrame] = []
	mr1b_columns: dict[str, str] | None = None

	for chunk in pd.read_csv(mr1b_filepath, chunksize=chunksize, low_memory=False):
		if mr1b_columns is None:
			mr1b_columns = _resolve_mr1b_columns(chunk.columns)

		chunk = chunk[
			[
				mr1b_columns['settlement_date'],
				mr1b_columns['settlement_period'],
				mr1b_columns['party_id'],
				mr1b_columns['energy_imbalance_vol'],
				mr1b_columns['credited_energy_vol']
			]
		].copy().rename(
			columns={
				mr1b_columns['settlement_date']: 'Settlement Date',
				mr1b_columns['settlement_period']: 'Settlement Period',
				mr1b_columns['party_id']: 'Party ID',
				mr1b_columns['energy_imbalance_vol']: 'Energy Imbalance Vol',
				mr1b_columns['credited_energy_vol']: 'CreditedEnergyVol'
			}
		)
		chunk['Party ID'] = chunk['Party ID'].astype(str).str.strip()
		chunk = chunk[chunk['Party ID'] != '']
		chunk['Settlement Date'] = pd.to_datetime(chunk['Settlement Date'], errors='coerce')
		in_range_mask = chunk['Settlement Date'].between(start_ts, end_ts)
		filtered_chunk = chunk.loc[in_range_mask].copy()
		if not filtered_chunk.empty:
			filtered_chunk['Settlement Date'] = filtered_chunk['Settlement Date'].dt.strftime('%Y-%m-%d')
			filtered_chunks.append(filtered_chunk)

	if not filtered_chunks:
		if mr1b_columns is None:
			raise ValueError('No MR1B data could be read from the provided filepath.')
		return pd.DataFrame(
			columns=[
				'Settlement Date',
				'Settlement Period',
				'Party ID',
				'Energy Imbalance Vol',
				'CreditedEnergyVol'
			]
		)

	combined = pd.concat(filtered_chunks, ignore_index=True)
	combined['Settlement Period'] = pd.to_numeric(combined['Settlement Period'], errors='coerce')
	combined['Energy Imbalance Vol'] = pd.to_numeric(combined['Energy Imbalance Vol'], errors='coerce')
	combined['CreditedEnergyVol'] = pd.to_numeric(combined['CreditedEnergyVol'], errors='coerce')
	combined = combined.dropna(subset=['Settlement Date', 'Settlement Period', 'Party ID'])

	# Keep only the first row per settlement date/period/party combination.
	combined = combined.drop_duplicates(
		subset=['Settlement Date', 'Settlement Period', 'Party ID'],
		keep='first'
	)
	combined['Settlement Period'] = combined['Settlement Period'].astype(int)

	return combined


async def recalculate_and_append_niv_to_imbalance_volume_data(
	imbalance_volume_filepath: str = IMBALANCE_VOLUME_DATA_FILEPATH,
	mr1b_filepath: str = MR1B_2025_ONWARDS_FILEPATH,
	bsc_roles_filepath: str = BSC_ROLES_FILEPATH,
	output_filepath: str | None = None,
	sheet_name: str | int = 0,
	strict_npt_mapping: bool = True,
	append_year: int = 2025,
	mr1b_chunksize: int = 250_000
) -> str:
	imbalance_volume_data = pd.read_excel(imbalance_volume_filepath, sheet_name=sheet_name)
	imbalance_volume_data = _normalise_imbalance_volume_columns(imbalance_volume_data)

	if imbalance_volume_data.empty:
		raise ValueError('No valid imbalance volume rows were found in the provided Excel file.')

	start_date = f'{append_year}-01-01'
	end_date = f'{append_year}-12-31'
	settlement_dates_and_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(
		start_date,
		end_date
	)

	bsc_roles_to_npt_mapping = get_bsc_id_to_npt_mapping(
		bsc_roles_filepath,
		strict_npt_mapping=strict_npt_mapping
	)
	mr1b_df = _load_mr1b_rows_for_date_range(
		mr1b_filepath,
		start_date,
		end_date,
		chunksize=mr1b_chunksize
	)

	missing_data: set[tuple[str, int]] = set()
	api_client = ApiClient()
	amv_niv_data = await recalculate_niv(
		settlement_dates_and_periods_per_day,
		mr1b_df,
		bsc_roles_to_npt_mapping,
		missing_data
	)
	zmv_niv_data = await recalculate_niv_zero_metered_volume(
		settlement_dates_and_periods_per_day,
		mr1b_df,
		bsc_roles_to_npt_mapping,
		missing_data,
		api_client
	)

	amv_niv_data = _normalise_settlement_keys(amv_niv_data)
	zmv_niv_data = _normalise_settlement_keys(zmv_niv_data)

	amv_columns = amv_niv_data[
		['settlement_date', 'settlement_period', 'net_imbalance_volume', 'counterfactual_niv']
	].drop_duplicates().rename(
		columns={
			'net_imbalance_volume': 'Factual',
			'counterfactual_niv': 'AMV'
		}
	)
	zmv_columns = zmv_niv_data[
		['settlement_date', 'settlement_period', 'counterfactual_niv']
	].drop_duplicates().rename(
		columns={'counterfactual_niv': 'ZMV'}
	)

	new_year_rows = amv_columns.merge(
		zmv_columns,
		on=['settlement_date', 'settlement_period'],
		how='left'
	)

	imbalance_volume_data['settlement_year'] = pd.to_datetime(
		imbalance_volume_data['settlement_date'],
		errors='coerce'
	).dt.year
	existing_data_without_append_year = imbalance_volume_data[
		imbalance_volume_data['settlement_year'] != append_year
	].drop(columns=['settlement_year'])

	for column in existing_data_without_append_year.columns:
		if column not in new_year_rows.columns:
			new_year_rows[column] = pd.NA
	for column in new_year_rows.columns:
		if column not in existing_data_without_append_year.columns:
			existing_data_without_append_year[column] = pd.NA

	new_year_rows = new_year_rows[existing_data_without_append_year.columns]
	combined_data = pd.concat(
		[existing_data_without_append_year, new_year_rows],
		ignore_index=True
	)
	combined_data = combined_data.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)

	if output_filepath is None:
		input_path = Path(imbalance_volume_filepath)
		output_directory = str(input_path.parent)
		output_filename = (
			f'{input_path.stem}_with_recalculated_niv_{start_date}_to_{end_date}.xlsx'
		)
		output_filepath = create_filepath(output_directory, output_filename)

	combined_data.to_excel(output_filepath, index=False)

	return output_filepath

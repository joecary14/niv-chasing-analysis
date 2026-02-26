import numpy as np
import pandas as pd
from pathlib import Path

from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day
from ancillary_files.excel_interaction import create_filepath
from data_collection.elexon_interaction import get_full_settlement_stacks_by_date_and_period
from elexonpy.api_client import ApiClient

BASE_FILEPATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/RNP/Analysis'


def _get_month_ranges(start_date: str, end_date: str) -> list[tuple[str, str]]:
	start = pd.to_datetime(start_date, errors='raise').normalize()
	end = pd.to_datetime(end_date, errors='raise').normalize()
	if start > end:
		raise ValueError('start_date must be on or before end_date')

	month_ranges = []
	current = start.replace(day=1)
	while current <= end:
		month_end = (current + pd.offsets.MonthEnd(0)).normalize()
		range_start = max(start, current)
		range_end = min(end, month_end)
		month_ranges.append((range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))
		current = (current + pd.offsets.MonthBegin(1)).normalize()

	return month_ranges


def _get_year_ranges(start_date: str, end_date: str) -> list[tuple[int, str, str]]:
	start = pd.to_datetime(start_date, errors='raise').normalize()
	end = pd.to_datetime(end_date, errors='raise').normalize()
	if start > end:
		raise ValueError('start_date must be on or before end_date')

	year_ranges = []
	for year in range(start.year, end.year + 1):
		year_start = pd.Timestamp(year=year, month=1, day=1)
		year_end = pd.Timestamp(year=year, month=12, day=31)
		range_start = max(start, year_start)
		range_end = min(end, year_end)
		year_ranges.append((year, range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d')))

	return year_ranges


def _normalise_so_flag(so_flag: pd.Series) -> pd.Series:
	if pd.api.types.is_bool_dtype(so_flag):
		return so_flag.fillna(False)

	so_flag_as_text = so_flag.astype(str).str.strip().str.lower()
	truthy_values = {'true', 't', '1', 'yes', 'y'}

	return so_flag_as_text.isin(truthy_values)


def _calculate_constraint_proxy_one_period(settlement_stack: pd.DataFrame) -> tuple[float, float, float]:
	if settlement_stack.empty or 'volume' not in settlement_stack.columns:
		return 0.0, 0.0, np.nan

	volumes = pd.to_numeric(settlement_stack['volume'], errors='coerce').fillna(0.0)
	total_absolute_volume = volumes.abs().sum()

	if 'so_flag' in settlement_stack.columns:
		so_flags = _normalise_so_flag(settlement_stack['so_flag'])
		constrained_absolute_volume = volumes[so_flags].abs().sum()
	else:
		constrained_absolute_volume = 0.0

	constraint_proxy = (
		constrained_absolute_volume / total_absolute_volume
		if total_absolute_volume > 0
		else np.nan
	)

	return constrained_absolute_volume, total_absolute_volume, constraint_proxy


async def calculate_constraint_proxy_by_period(
	start_date: str,
	end_date: str,
	api_client: ApiClient | None = None,
	output_filename: str | None = None
) -> pd.DataFrame:
	year_ranges = _get_year_ranges(start_date, end_date)
	if not year_ranges:
		return pd.DataFrame(
			columns=[
				'settlement_date',
				'settlement_period',
				'constrained_absolute_volume',
				'total_absolute_volume',
				'constraint_proxy'
			]
		)

	client = api_client or ApiClient()
	base_output_file = output_filename or f'constraint_proxy_{start_date}_to_{end_date}.xlsx'
	base_stem = Path(base_output_file).stem

	all_years_dataframes = []
	for year, year_start_date, year_end_date in year_ranges:
		yearly_rows = []
		month_ranges = _get_month_ranges(year_start_date, year_end_date)
		for month_start_date, month_end_date in month_ranges:
			settlement_dates_and_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(
				month_start_date,
				month_end_date
			)
			if not settlement_dates_and_periods_per_day:
				continue

			missing_data_points = set()
			settlement_stacks = await get_full_settlement_stacks_by_date_and_period(
				client,
				settlement_dates_and_periods_per_day,
				missing_data_points
			)

			for settlement_date, periods_in_day in settlement_dates_and_periods_per_day.items():
				for settlement_period in range(1, periods_in_day + 1):
					settlement_stack = settlement_stacks.get((settlement_date, settlement_period), pd.DataFrame())
					constrained_absolute_volume, total_absolute_volume, constraint_proxy = _calculate_constraint_proxy_one_period(
						settlement_stack
					)
					yearly_rows.append(
						{
							'settlement_date': settlement_date,
							'settlement_period': settlement_period,
							'constrained_absolute_volume': constrained_absolute_volume,
							'total_absolute_volume': total_absolute_volume,
							'constraint_proxy': constraint_proxy
						}
					)

			print(f'Completed constraint analysis for {month_start_date[:7]}')

		yearly_df = pd.DataFrame(yearly_rows)
		if not yearly_df.empty:
			yearly_df = yearly_df.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)

		yearly_output_file = f'{base_stem}_{year}.xlsx'
		yearly_output_filepath = create_filepath(BASE_FILEPATH, yearly_output_file)
		yearly_df.to_excel(yearly_output_filepath, index=False)
		print(f'Constraint proxy yearly output saved to: {yearly_output_filepath}')

		all_years_dataframes.append(yearly_df)

	if all_years_dataframes:
		proxy_by_period_df = pd.concat(all_years_dataframes, ignore_index=True)
		if not proxy_by_period_df.empty:
			proxy_by_period_df = proxy_by_period_df.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)
	else:
		proxy_by_period_df = pd.DataFrame(
			columns=[
				'settlement_date',
				'settlement_period',
				'constrained_absolute_volume',
				'total_absolute_volume',
				'constraint_proxy'
			]
		)

	output_filepath = create_filepath(BASE_FILEPATH, base_output_file)
	proxy_by_period_df.to_excel(output_filepath, index=False)
	print(f'Constraint proxy aggregated output saved to: {output_filepath}')

	return proxy_by_period_df


def calculate_overall_constraint_proxy(
	constraint_proxy_by_period: pd.DataFrame
) -> float:
	if constraint_proxy_by_period.empty:
		return np.nan

	constrained_total = pd.to_numeric(
		constraint_proxy_by_period['constrained_absolute_volume'], errors='coerce'
	).fillna(0.0).sum()
	total_actions_volume = pd.to_numeric(
		constraint_proxy_by_period['total_absolute_volume'], errors='coerce'
	).fillna(0.0).sum()

	if total_actions_volume <= 0:
		return np.nan

	return constrained_total / total_actions_volume

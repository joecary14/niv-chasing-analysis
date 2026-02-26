import asyncio
from datetime import timedelta

import pandas as pd

from ancillary_files.datetime_functions import get_settlement_dates_and_settlement_periods_per_day
from ancillary_files.excel_interaction import create_filepath
from data_collection.elexon_interaction import (
	get_bid_offer_acceptance_data,
	get_full_settlement_stacks_by_date_and_period,
	get_niv_data,
)
from data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
from elexonpy.api_client import ApiClient
from gb_analysis.system_price_from_stack import get_new_system_price


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


async def get_market_wide_bid_offer_acceptances_by_period(
	settlement_dates_with_periods_per_day: dict[str, int],
	api_client: ApiClient
) -> dict[tuple[str, int], pd.DataFrame]:
	tasks = [
		get_bid_offer_acceptance_data(settlement_date, settlement_periods_in_day, api_client)
		for settlement_date, settlement_periods_in_day in settlement_dates_with_periods_per_day.items()
	]
	daily_results = await asyncio.gather(*tasks)

	bid_offer_acceptances_by_period = {}
	for one_day_result in daily_results:
		bid_offer_acceptances_by_period.update(one_day_result)

	return bid_offer_acceptances_by_period


async def get_market_wide_boas_and_imbalance_settlement_stacks_by_period(
	start_date: str,
	end_date: str,
	api_client: ApiClient | None = None
) -> tuple[dict[tuple[str, int], pd.DataFrame], dict[tuple[str, int], pd.DataFrame]]:
	month_ranges = _get_month_ranges(start_date, end_date)
	if not month_ranges:
		return {}, {}

	client = api_client or ApiClient()
	bid_offer_acceptances_by_period = {}
	settlement_stacks_by_period = {}

	for month_start_date, month_end_date in month_ranges:
		settlement_dates_with_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(
			month_start_date,
			month_end_date
		)
		if not settlement_dates_with_periods_per_day:
			continue

		missing_data_points = set()
		bid_offer_acceptances_task = get_market_wide_bid_offer_acceptances_by_period(
			settlement_dates_with_periods_per_day,
			client
		)
		settlement_stacks_task = get_full_settlement_stacks_by_date_and_period(
			client,
			settlement_dates_with_periods_per_day,
			missing_data_points
		)
		month_bid_offer_acceptances_by_period, month_settlement_stacks_by_period = await asyncio.gather(
			bid_offer_acceptances_task,
			settlement_stacks_task
		)
		bid_offer_acceptances_by_period.update(month_bid_offer_acceptances_by_period)
		settlement_stacks_by_period.update(month_settlement_stacks_by_period)

		print(f'Completed BOA and stack retrieval for {month_start_date[:7]}')

	return bid_offer_acceptances_by_period, settlement_stacks_by_period


def _normalise_bool_series(series: pd.Series) -> pd.Series:
	if pd.api.types.is_bool_dtype(series):
		return series.fillna(False)
	values = series.astype(str).str.strip().str.lower()
	return values.isin({'true', 't', '1', 'yes', 'y'})


def _resolve_cadl_duration_threshold(cadl_parameter: pd.Timedelta | timedelta | int | float | str) -> pd.Timedelta:
	if isinstance(cadl_parameter, pd.Timedelta):
		return cadl_parameter
	if isinstance(cadl_parameter, timedelta):
		return pd.Timedelta(cadl_parameter)
	if isinstance(cadl_parameter, (int, float)):
		return pd.Timedelta(minutes=float(cadl_parameter))
	if isinstance(cadl_parameter, str):
		return pd.to_timedelta(cadl_parameter)
	raise ValueError('cadl_parameter must be a pandas Timedelta, datetime.timedelta, numeric minutes, or timedelta string.')


def _normalise_identifier(series: pd.Series) -> tuple[pd.Series, pd.Series]:
	numeric_identifier = pd.to_numeric(series, errors='coerce')
	text_identifier = series.astype(str).str.strip()
	return numeric_identifier, text_identifier


def _build_acceptance_duration_maps(bid_offer_acceptances_one_period: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
	if bid_offer_acceptances_one_period.empty:
		return pd.Series(dtype='timedelta64[ns]'), pd.Series(dtype='timedelta64[ns]')
	if 'acceptance_number' not in bid_offer_acceptances_one_period.columns:
		return pd.Series(dtype='timedelta64[ns]'), pd.Series(dtype='timedelta64[ns]')
	if 'time_from' not in bid_offer_acceptances_one_period.columns or 'time_to' not in bid_offer_acceptances_one_period.columns:
		return pd.Series(dtype='timedelta64[ns]'), pd.Series(dtype='timedelta64[ns]')

	boas = bid_offer_acceptances_one_period.copy()
	boas['time_from'] = pd.to_datetime(boas['time_from'], errors='coerce', utc=True)
	boas['time_to'] = pd.to_datetime(boas['time_to'], errors='coerce', utc=True)
	boas['action_duration'] = boas['time_to'] - boas['time_from']
	boas = boas.dropna(subset=['acceptance_number', 'action_duration'])
	if boas.empty:
		return pd.Series(dtype='timedelta64[ns]'), pd.Series(dtype='timedelta64[ns]')

	boas['acceptance_number_numeric'], boas['acceptance_number_text'] = _normalise_identifier(boas['acceptance_number'])
	numeric_duration_map = (
		boas.dropna(subset=['acceptance_number_numeric'])
		.drop_duplicates(subset=['acceptance_number_numeric'])
		.set_index('acceptance_number_numeric')['action_duration']
	)
	text_duration_map = (
		boas.drop_duplicates(subset=['acceptance_number_text'])
		.set_index('acceptance_number_text')['action_duration']
	)

	return numeric_duration_map, text_duration_map


def apply_cadl_duration_filter_to_settlement_stacks_by_period(
	settlement_stacks_by_period: dict[tuple[str, int], pd.DataFrame],
	bid_offer_acceptances_by_period: dict[tuple[str, int], pd.DataFrame],
	cadl_parameter: pd.Timedelta | timedelta | int | float | str
) -> dict[tuple[str, int], pd.DataFrame]:
	cadl_duration_threshold = _resolve_cadl_duration_threshold(cadl_parameter)
	adjusted_settlement_stacks_by_period: dict[tuple[str, int], pd.DataFrame] = {}

	for key, settlement_stack_one_period in settlement_stacks_by_period.items():
		adjusted_stack = settlement_stack_one_period.copy()
		if adjusted_stack.empty:
			adjusted_settlement_stacks_by_period[key] = adjusted_stack
			continue
		if 'cadl_flag' not in adjusted_stack.columns or 'acceptance_id' not in adjusted_stack.columns:
			adjusted_settlement_stacks_by_period[key] = adjusted_stack
			continue

		cadl_mask = _normalise_bool_series(adjusted_stack['cadl_flag'])
		if not cadl_mask.any():
			adjusted_settlement_stacks_by_period[key] = adjusted_stack
			continue

		bid_offer_acceptances_one_period = bid_offer_acceptances_by_period.get(key, pd.DataFrame())
		numeric_duration_map, text_duration_map = _build_acceptance_duration_maps(bid_offer_acceptances_one_period)
		if numeric_duration_map.empty and text_duration_map.empty:
			adjusted_settlement_stacks_by_period[key] = adjusted_stack
			continue

		cadl_rows = adjusted_stack.loc[cadl_mask, ['acceptance_id']].copy()
		cadl_rows['acceptance_id_numeric'], cadl_rows['acceptance_id_text'] = _normalise_identifier(cadl_rows['acceptance_id'])
		matched_duration = cadl_rows['acceptance_id_numeric'].map(numeric_duration_map)
		matched_duration = matched_duration.where(
			matched_duration.notna(),
			cadl_rows['acceptance_id_text'].map(text_duration_map)
		)

		set_false_mask = matched_duration < cadl_duration_threshold
		indices_to_set_false = matched_duration.index[set_false_mask.fillna(False)]
		adjusted_stack.loc[indices_to_set_false, 'cadl_flag'] = False
		adjusted_settlement_stacks_by_period[key] = adjusted_stack

	return adjusted_settlement_stacks_by_period


async def get_cadl_adjusted_settlement_stacks_by_period(
	start_date: str,
	end_date: str,
	cadl_parameter: pd.Timedelta | timedelta | int | float | str,
	api_client: ApiClient | None = None
) -> dict[tuple[str, int], pd.DataFrame]:
	month_ranges = _get_month_ranges(start_date, end_date)
	if not month_ranges:
		return {}

	all_adjusted_settlement_stacks_by_period = {}
	for month_start_date, month_end_date in month_ranges:
		bid_offer_acceptances_by_period, settlement_stacks_by_period = (
			await get_market_wide_boas_and_imbalance_settlement_stacks_by_period(
				month_start_date,
				month_end_date,
				api_client=api_client
			)
		)

		month_adjusted_settlement_stacks_by_period = apply_cadl_duration_filter_to_settlement_stacks_by_period(
			settlement_stacks_by_period,
			bid_offer_acceptances_by_period,
			cadl_parameter
		)
		all_adjusted_settlement_stacks_by_period.update(month_adjusted_settlement_stacks_by_period)
		print(f'Completed CADL stack adjustment for {month_start_date[:7]}')

	return all_adjusted_settlement_stacks_by_period


async def export_cadl_recalculated_system_sell_prices(
	start_date: str,
	end_date: str,
	cadl_parameter: pd.Timedelta | timedelta | int | float | str,
	output_filename: str | None = None,
	api_client: ApiClient | None = None
) -> str:
	month_ranges = _get_month_ranges(start_date, end_date)
	if not month_ranges:
		raise ValueError('No settlement dates found in the provided date range.')

	client = api_client or ApiClient()
	recalculated_rows = []
	for month_start_date, month_end_date in month_ranges:
		settlement_dates_with_periods_per_day = get_settlement_dates_and_settlement_periods_per_day(
			month_start_date,
			month_end_date
		)
		if not settlement_dates_with_periods_per_day:
			continue

		missing_data_points = set()
		ancillary_price_data_task = get_ancillary_price_data_for_sp_calculation(
			client,
			settlement_dates_with_periods_per_day,
			missing_data_points
		)
		niv_data_task = get_niv_data(settlement_dates_with_periods_per_day, client)
		cadl_adjusted_stacks_task = get_cadl_adjusted_settlement_stacks_by_period(
			month_start_date,
			month_end_date,
			cadl_parameter,
			api_client=client
		)
		ancillary_price_data, niv_data, cadl_adjusted_stacks_by_period = await asyncio.gather(
			ancillary_price_data_task,
			niv_data_task,
			cadl_adjusted_stacks_task
		)

		ancillary_price_data = ancillary_price_data.copy()
		ancillary_price_data['settlement_date'] = pd.to_datetime(
			ancillary_price_data['settlement_date'], errors='coerce'
		).dt.strftime('%Y-%m-%d')
		niv_data = niv_data.copy()
		niv_data['settlement_date'] = pd.to_datetime(niv_data['settlement_date'], errors='coerce').dt.strftime('%Y-%m-%d')

		price_lookup = ancillary_price_data.set_index(['settlement_date', 'settlement_period'])
		niv_lookup = niv_data.set_index(['settlement_date', 'settlement_period'])

		for (settlement_date, settlement_period), settlement_stack in cadl_adjusted_stacks_by_period.items():
			system_sell_price = None
			net_imbalance_volume = None
			if (settlement_date, settlement_period) in niv_lookup.index:
				niv_row = niv_lookup.loc[(settlement_date, settlement_period)]
				if isinstance(niv_row, pd.DataFrame):
					niv_row = niv_row.iloc[0]
				system_sell_price = niv_row.get('system_sell_price')
				net_imbalance_volume = niv_row.get('net_imbalance_volume')

			recalculated_system_sell_price = None
			if (settlement_date, settlement_period) in price_lookup.index:
				price_row = price_lookup.loc[(settlement_date, settlement_period)]
				if isinstance(price_row, pd.DataFrame):
					price_row = price_row.iloc[0]
				market_index_price = price_row.get('vwap_midp')
				if net_imbalance_volume is not None and pd.notna(net_imbalance_volume):
					price_adjustment_column = (
						'buy_price_price_adjustment'
						if net_imbalance_volume > 0
						else 'sell_price_price_adjustment'
					)
					price_adjustment = price_row.get(price_adjustment_column, 0)
					recalculated_system_sell_price = get_new_system_price(
						settlement_stack,
						price_adjustment,
						market_index_price,
						{},
						net_imbalance_volume
					)

			recalculated_rows.append(
				{
					'settlement_date': settlement_date,
					'settlement_period': settlement_period,
					'system_sell_price': system_sell_price,
					'recalculated_system_sell_price': recalculated_system_sell_price
				}
			)

		print(f'Completed CADL system price recalculation for {month_start_date[:7]}')

	results_df = pd.DataFrame(recalculated_rows)
	results_df = results_df.sort_values(['settlement_date', 'settlement_period']).reset_index(drop=True)

	default_output_filename = f'cadl_system_sell_prices_{start_date}_to_{end_date}.xlsx'
	output_filepath = create_filepath(BASE_FILEPATH, output_filename or default_output_filename)
	results_df.to_excel(output_filepath, index=False)

	return output_filepath

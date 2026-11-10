"""
Implied system price as BOAs arrive in real time.

For a single settlement period, the final settlement stack is only known after
the period has finished. At any moment *during* the period, however, the BOAs
accepted so far make up a partial version of that final stack. This module:

1. fetches the BOAs and the final settlement stack for the period,
2. for each BOA acceptance time, keeps the stack rows belonging to BOAs accepted
   up to that time (plus rows with no acceptance id, e.g. BSAD, which are
   treated as known from the start of the period),
3. runs the existing price workflow (gb_analysis.system_price_from_stack) on that
   partial stack, and
4. plots the resulting implied system price against frequency.

Run from the repository root with:
    python -m rnp_analysis.implied_system_price_analysis
"""
import asyncio

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from elexonpy.api_client import ApiClient

from ancillary_files.datetime_functions import get_settlement_date_period_to_utc_start_time_mapping
from data_collection.elexon_interaction import get_niv_data
from data_processing.price_data_processing import get_ancillary_price_data_for_sp_calculation
from gb_analysis.system_price_from_stack import get_new_system_price
from rnp_analysis.frequency_deviation_analysis import (
    get_boa_volumes_for_period,
    get_boas_for_settlement_period,
)

SETTLEMENT_DATE = '2025-11-08'
SETTLEMENT_PERIOD = 42
AXIS_LABEL_FONT_SIZE = 16

# TODO: set these. The frequency file follows the naming of the September one.
FREQUENCY_FILE_PATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/Side Projects/RNP/Analysis/Analysis/Frequency Analysis/Nov 2025 Frequency.csv'


async def plot_implied_system_price(
    settlement_date: str = SETTLEMENT_DATE,
    settlement_period: int = SETTLEMENT_PERIOD,
    frequency_file_path: str = FREQUENCY_FILE_PATH,
) -> None:
    mapping = get_settlement_date_period_to_utc_start_time_mapping([int(settlement_date[:4])])
    start_time = mapping[(settlement_date, settlement_period)]
    end_time = start_time + pd.Timedelta(minutes=30)

    api_client = ApiClient()
    boas, full_stack, price_inputs = await asyncio.gather(
        get_boas_for_settlement_period(settlement_date, settlement_period),
        get_full_settlement_stack(settlement_date, settlement_period),
        get_price_inputs(api_client, settlement_date, settlement_period),
    )
    market_index_price, buy_price_adjustment, sell_price_adjustment, published_system_price = price_inputs

    # The settlement stack already carries each BMU's TLM, so no external TLM file is needed.
    tlm_by_bmu = (
        full_stack.dropna(subset=['transmission_loss_multiplier'])
        .drop_duplicates('id')
        .set_index('id')['transmission_loss_multiplier']
        .to_dict()
    )

    implied_prices = get_implied_system_price_series(
        boas,
        full_stack,
        start_time,
        end_time,
        market_index_price,
        buy_price_adjustment,
        sell_price_adjustment,
        tlm_by_bmu,
    )

    final_implied_price = implied_prices.iloc[-1]
    print(f'Implied system price from the complete stack: {final_implied_price:.2f}')
    print(f'Published system price:                        {published_system_price:.2f}')

    frequency_data = get_frequency_data(frequency_file_path, start_time, end_time)

    plot_frequency_and_implied_price(
        frequency_data, implied_prices, published_system_price, start_time, end_time,
    )


def get_implied_system_price_series(
    boas: pd.DataFrame,
    full_stack: pd.DataFrame,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    market_index_price: float,
    buy_price_adjustment: float,
    sell_price_adjustment: float,
    tlm_by_bmu: dict[str, float],
) -> pd.Series:
    """
    Implied system price after each BOA accepted within [start_time, end_time).

    The series starts at `start_time` (BOAs accepted before the period only) and
    gains one point per distinct acceptance time. It is extended to `end_time`
    so a step plot spans the whole window. The last point before `end_time` is
    the price implied by every BOA in the period.
    """
    boas = boas.copy()
    boas['acceptance_time'] = pd.to_datetime(boas['acceptance_time'], utc=True, errors='coerce')
    boas = boas.dropna(subset=['acceptance_time', 'acceptance_number'])

    pre_period_ids = set(boas.loc[boas['acceptance_time'] < start_time, 'acceptance_number'])
    real_time_boas = boas[(boas['acceptance_time'] >= start_time) & (boas['acceptance_time'] < end_time)]

    def price_for(accepted_ids: set) -> float:
        partial_stack = get_partial_stack(full_stack, accepted_ids)
        return get_implied_system_price(
            partial_stack, market_index_price, buy_price_adjustment, sell_price_adjustment, tlm_by_bmu,
        )

    accepted_ids = set(pre_period_ids)
    times = [start_time]
    prices = [price_for(accepted_ids)]
    for acceptance_time, boas_at_time in real_time_boas.groupby('acceptance_time'):
        accepted_ids |= set(boas_at_time['acceptance_number'])
        times.append(acceptance_time)
        prices.append(price_for(accepted_ids))

    times.append(end_time)
    prices.append(prices[-1])

    return pd.Series(prices, index=pd.DatetimeIndex(times), name='implied_system_price')


def get_partial_stack(full_stack: pd.DataFrame, accepted_ids: set) -> pd.DataFrame:
    """Rows of the final stack belonging to the given BOAs, plus rows with no acceptance id."""
    if full_stack.empty:
        return full_stack.copy()
    mask = full_stack['acceptance_id'].isin(accepted_ids) | full_stack['acceptance_id'].isna()
    return full_stack[mask].reset_index(drop=True)


def get_implied_system_price(
    partial_stack: pd.DataFrame,
    market_index_price: float,
    buy_price_adjustment: float,
    sell_price_adjustment: float,
    tlm_by_bmu: dict[str, float],
) -> float:
    """System price implied by a (partial) settlement stack, using the existing workflow."""
    if partial_stack.empty:
        return market_index_price

    # Which side sets the price (and which adjustment applies) follows the sign of the
    # stack's net volume, as in the P462 workflow.
    net_volume = partial_stack['volume'].sum()
    price_adjustment = buy_price_adjustment if net_volume > 0 else sell_price_adjustment

    try:
        return get_new_system_price(
            partial_stack.copy(), price_adjustment, market_index_price, tlm_by_bmu, net_volume,
        )
    except KeyError:
        print(f'Fallback to market index price for partial stack of {len(partial_stack)} rows (KeyError)')
        # The existing workflow assumes both a buy and a sell set exist; early in the period
        # (e.g. only offers accepted so far) one can be empty.
        return market_index_price


async def get_full_settlement_stack(settlement_date: str, settlement_period: int) -> pd.DataFrame:
    # Same bid + offer stack as the NIV analysis; already requested there as `boa_volumes`.
    return await get_boa_volumes_for_period(settlement_date, settlement_period)


async def get_price_inputs(
    api_client: ApiClient,
    settlement_date: str,
    settlement_period: int,
) -> tuple[float, float, float, float]:
    """Market index price, buy/sell price adjustments and the published system price."""
    periods_per_day = {settlement_date: 48}
    ancillary = await get_ancillary_price_data_for_sp_calculation(api_client, periods_per_day, set())
    ancillary['settlement_date'] = pd.to_datetime(ancillary['settlement_date']).dt.strftime('%Y-%m-%d')
    row = ancillary[
        (ancillary['settlement_date'] == settlement_date)
        & (ancillary['settlement_period'] == settlement_period)
    ].iloc[0]

    system_prices = await get_niv_data(periods_per_day, api_client)
    system_prices['settlement_date'] = pd.to_datetime(system_prices['settlement_date']).dt.strftime('%Y-%m-%d')
    published = system_prices[
        (system_prices['settlement_date'] == settlement_date)
        & (system_prices['settlement_period'] == settlement_period)
    ]['system_sell_price'].iloc[0]

    return (
        float(row['vwap_midp']),
        float(row['buy_price_price_adjustment']),
        float(row['sell_price_price_adjustment']),
        float(published),
    )


def get_frequency_data(
    frequency_file_path: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> pd.DataFrame:
    frequency_data = pd.read_csv(frequency_file_path)
    frequency_data['dtm'] = pd.to_datetime(frequency_data['dtm'], utc=True, errors='coerce')
    frequency_data = frequency_data.dropna(subset=['dtm']).sort_values('dtm')
    return frequency_data[(frequency_data['dtm'] >= start_time) & (frequency_data['dtm'] < end_time)]


def plot_frequency_and_implied_price(
    frequency_data: pd.DataFrame,
    implied_prices: pd.Series,
    published_system_price: float,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> None:
    fig, ax_freq = plt.subplots(figsize=(12, 8))
    ax_price = ax_freq.twinx()

    ax_freq.plot(frequency_data['dtm'], frequency_data['f'], color='tab:blue')
    ax_freq.axhline(50.0, color='black', linestyle='-')
    ax_freq.set_ylabel('Frequency (Hz)', color='tab:blue', fontsize=AXIS_LABEL_FONT_SIZE)
    ax_freq.tick_params(axis='y', labelcolor='tab:blue')
    ax_freq.grid(False)

    ax_price.step(implied_prices.index, implied_prices.values, where='post', color='tab:orange')
    ax_price.axhline(
        published_system_price, color='tab:orange', linestyle=':',
        label=f'Published system price ({published_system_price:.2f})',
    )
    ax_price.set_ylabel('Implied system price (£/MWh)', color='tab:orange', fontsize=AXIS_LABEL_FONT_SIZE)
    ax_price.tick_params(axis='y', labelcolor='tab:orange')

    # Centre the outturn (published) price on the right-hand axis.
    max_deviation = max(abs(implied_prices - published_system_price).max(), 1.0)
    padding = 1.05 * max_deviation
    ax_price.set_ylim(published_system_price - padding, published_system_price + padding)

    ax_freq.set_xlim(start_time, end_time)
    ax_freq.set_xlabel('Time (UTC)', fontsize=AXIS_LABEL_FONT_SIZE)
    ax_freq.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax_freq.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.setp(ax_freq.get_xticklabels(), rotation=90)

    handles = ax_freq.get_legend_handles_labels()[0] + ax_price.get_legend_handles_labels()[0]
    labels = ax_freq.get_legend_handles_labels()[1] + ax_price.get_legend_handles_labels()[1]
    ax_freq.legend(handles, labels, loc='upper left')

    fig.tight_layout()
    plt.show()


if __name__ == '__main__':
    asyncio.run(plot_implied_system_price())

import asyncio
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from elexonpy.api_client import ApiClient
from elexonpy.api import BidOfferAcceptancesApi, IndicativeImbalanceSettlementApi
from ancillary_files.datetime_functions import get_settlement_date_period_to_utc_start_time_mapping

FREQUENCY_FILE_PATH = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Second Year/Side Projects/RNP/Analysis/Analysis/Frequency Analysis/Sep 2025 Frequency.csv'

async def plot_frequency_repsonse(
    settlement_date: str,
    settlement_period: int,
) -> None:
    mapping = get_settlement_date_period_to_utc_start_time_mapping([int(settlement_date[:4])])
    start_time = mapping[(settlement_date, settlement_period)]
    end_time = start_time + pd.Timedelta(minutes=30)
    analysis_data = await get_data_for_analysis(settlement_date, settlement_period, start_time, end_time)
    boas = analysis_data['boas']
    acceptance_volumes = analysis_data['boa_volumes']

    boas = boas.copy()
    boas['acceptance_time'] = pd.to_datetime(boas['acceptance_time'], utc=True, errors='coerce')
    boas = boas.dropna(subset=['acceptance_time']).sort_values('acceptance_time')

    real_time_boas = boas[
        (boas['acceptance_time'] >= start_time) & (boas['acceptance_time'] < end_time)
    ]

    pre_period_boas = boas[
        boas['acceptance_time'] < start_time
    ]

    pre_period_boa_ids = pre_period_boas['acceptance_number'].unique()
    
    pre_period_acceptance_volumes = acceptance_volumes[
        acceptance_volumes['acceptance_id'].isin(pre_period_boa_ids)
        | acceptance_volumes['acceptance_id'].isna()
    ]
    
    real_time_acceptance_volumes = acceptance_volumes[
        acceptance_volumes['acceptance_id'].isin(real_time_boas['acceptance_number'].unique())
    ]

    pre_period_niv = pre_period_acceptance_volumes['arbitrage_adjusted_volume'].sum()

    real_time_niv = get_real_time_niv(
        real_time_boas,
        acceptance_volumes,
        pre_period_niv,
        start_time,
        end_time,
    )

    frequency_data = analysis_data['frequency_data'].copy()
    frequency_data['dtm'] = pd.to_datetime(frequency_data['dtm'], utc=True, errors='coerce')
    frequency_data = frequency_data.dropna(subset=['dtm']).sort_values('dtm')

    real_time_acceptance_times = real_time_boas['acceptance_time'].dropna().sort_values()

    fig, ax_freq = plt.subplots(
        figsize=(12, 8),
    )

    ax_niv = ax_freq.twinx()

    ax_freq.plot(
        frequency_data['dtm'],
        frequency_data['f'],
        color='tab:blue',
    )
    ax_freq.set_ylabel('Frequency', color='tab:blue')
    ax_freq.axhline(50.0, color='black', linestyle='-')
    ax_freq.tick_params(axis='y', labelcolor='tab:blue')
    ax_freq.grid(False)

    ax_niv.step(
        real_time_niv.index,
        real_time_niv.values,
        where='post',
        color='tab:orange',
    )
    ax_niv.set_ylabel('NIV', color='tab:orange')
    ax_niv.axhline(0.0, color='black', linestyle='-')
    ax_niv.tick_params(axis='y', labelcolor='tab:orange')

    # Align 50 Hz on the left axis with 0 NIV on the right axis.
    freq_min, freq_max = ax_freq.get_ylim()
    niv_min, niv_max = ax_niv.get_ylim()

    freq_fraction = (50.0 - freq_min) / (freq_max - freq_min)

    niv_span_below = max(abs(niv_min), 1e-9)
    niv_span_above = max(abs(niv_max), 1e-9)

    required_upper = niv_span_below * (1.0 - freq_fraction) / max(freq_fraction, 1e-9)
    required_lower = niv_span_above * freq_fraction / max(1.0 - freq_fraction, 1e-9)

    ax_niv.set_ylim(
        -max(niv_span_below, required_lower),
        max(niv_span_above, required_upper),
    )

    for acceptance_time in real_time_acceptance_times:
        ax_freq.axvline(
            acceptance_time,
            linestyle='--',
            color='grey',
        )
    ax_freq.plot([], [], linestyle='--', color='grey', label='BOAs')
    ax_freq.set_xlim(start_time, end_time)
    ax_freq.legend(loc='upper left')

    ax_freq.set_xlabel('Time (UTC)')

    ax_freq.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax_freq.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.setp(ax_freq.get_xticklabels(), rotation=90)

    fig.tight_layout()
    plt.show()

async def get_data_for_analysis(
    settlement_date: str,
    settlement_period: int,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp
) -> dict[str, pd.DataFrame]:
    boas = await get_boas_for_settlement_period(settlement_date, settlement_period)
    boa_volumes = await get_boa_volumes_for_period(settlement_date, settlement_period)
    frequency_data = get_frequency_data_for_settlement_period(start_time, end_time)

    return {
        "boas": boas,
        "boa_volumes": boa_volumes,
        "frequency_data": frequency_data
    }

async def get_boa_volumes_for_period(
    settlement_date: str,
    settlement_period: int
) -> pd.DataFrame:
    api_client = ApiClient()
    imb_api = IndicativeImbalanceSettlementApi(api_client)
    
    bid_task = imb_api.balancing_settlement_stack_all_bid_offer_settlement_date_settlement_period_get(
        bid_offer="bid",
        settlement_date=settlement_date,
        settlement_period=settlement_period,
        format='dataframe',
        async_req=True
    )
    
    offer_task = imb_api.balancing_settlement_stack_all_bid_offer_settlement_date_settlement_period_get(
        bid_offer="offer",
        settlement_date=settlement_date,
        settlement_period=settlement_period,
        format='dataframe',
        async_req=True
    )
    
    tasks = [bid_task, offer_task]
    
    bid_data, offer_data = await asyncio.gather(*[asyncio.to_thread(task.get) for task in tasks])
    
    data = pd.concat([bid_data, offer_data], ignore_index=True)
    
    return data

async def get_boas_for_settlement_period(
    settlement_date: str,
    settlement_period: int
) -> pd.DataFrame:
    api_client = ApiClient()
    boa_api = BidOfferAcceptancesApi(api_client)
    
    task = boa_api.balancing_acceptances_all_get(
            settlement_date=settlement_date,
            settlement_period=settlement_period,
            format='dataframe',
            async_req=True
        )
    
    boas = await asyncio.to_thread(task.get)
    
    return boas

def get_frequency_data_for_settlement_period(
    start_time: pd.Timestamp,
    end_time: pd.Timestamp
) -> pd.DataFrame:
    
    frequency_data = pd.read_csv((FREQUENCY_FILE_PATH))
    frequency_data['dtm'] = pd.to_datetime(frequency_data['dtm'], utc=True, errors='coerce')

    period_data = frequency_data[
        (frequency_data['dtm'] >= start_time) & 
        (frequency_data['dtm'] < end_time)
    ]
    
    return period_data

def get_real_time_niv(
    real_time_boas: pd.DataFrame,
    acceptance_volumes: pd.DataFrame,
    pre_period_niv: float,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> pd.Series:
    """
    Calculate NIV as BOAs arrive in real time.

    The returned series begins at `start_time` with `pre_period_niv`
    and is extended to `end_time` so the step plot spans the full
    frequency window.
    """

    if real_time_boas is None or real_time_boas.empty:
        return pd.Series(
            [pre_period_niv, pre_period_niv],
            index=pd.DatetimeIndex([start_time, end_time]),
            name='niv',
        )

    if acceptance_volumes is None or acceptance_volumes.empty:
        real_time_times = pd.to_datetime(
            real_time_boas['acceptance_time'],
            utc=True,
            errors='coerce',
        ).dropna().sort_values()
        index = pd.DatetimeIndex([start_time, *real_time_times.tolist(), end_time])
        values = [pre_period_niv] + [pre_period_niv] * (len(real_time_times) + 1)
        return pd.Series(values, index=index, name='niv')

    boas_frame = real_time_boas.copy()
    volumes_frame = acceptance_volumes.copy()

    boas_frame['acceptance_time'] = pd.to_datetime(
        boas_frame['acceptance_time'],
        utc=True,
        errors='coerce',
    )
    boas_frame = boas_frame.dropna(subset=['acceptance_time', 'acceptance_number']).sort_values('acceptance_time')

    volumes_by_id = (
        volumes_frame.groupby('acceptance_id', as_index=True)['arbitrage_adjusted_volume']
        .sum()
    )

    current_niv = float(pre_period_niv)
    niv_values: list[float] = [current_niv]
    niv_index: list[pd.Timestamp] = [pd.to_datetime(start_time, utc=True)]
    unique_boas = boas_frame.drop_duplicates(subset=['acceptance_number'])

    for _, boa in unique_boas.iterrows():
        acceptance_id = boa['acceptance_number']
        accepted_volume = float(volumes_by_id.get(acceptance_id, 0.0))
        current_niv += accepted_volume
        niv_index.append(boa['acceptance_time'])
        niv_values.append(current_niv)

    if pd.to_datetime(niv_index[-1], utc=True) < pd.to_datetime(end_time, utc=True):
        niv_index.append(pd.to_datetime(end_time, utc=True))
        niv_values.append(current_niv)

    return pd.Series(niv_values, index=pd.DatetimeIndex(niv_index), name='niv')
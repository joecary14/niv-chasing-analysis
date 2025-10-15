import ancillary_files.datetime_functions as datetime_functions
import isem_analysis.api_interaction as api_interaction
import isem_analysis.imbalance_volume_calculator as imbalance_volume_calculator
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np

async def calculate_imbalance_volumes(
    start_date: str,
    end_date: str
) -> None:
    dates = datetime_functions.generate_settlement_dates(start_date, end_date, True)
    imbalance_data = await api_interaction.collect_data_from_api(dates, dpug_ids=['BM-103', 'BM-084'])
    imbalance_price_data = await api_interaction.get_bm_026_data(dates)
    exchange_rate_data = await api_interaction.get_bm_084_data(dates)
    
    imbalance_volumes = imbalance_volume_calculator.calculate_imbalance_volume_by_unit(
        raw_bm_103_data_by_date=imbalance_data['BM-103'],
        imbalance_price_data=imbalance_price_data,
        exchange_rate_data=exchange_rate_data
    )

    expected_datetimes = []
    for date in dates:
        date_obj = pd.to_datetime(date)
        for period in range(48):
            timestamp = pd.to_datetime(date_obj + pd.Timedelta(minutes=30 * period), utc=True)
            expected_datetimes.append(timestamp)
    
    expected_set = set(expected_datetimes)
    actual_set = set(imbalance_volumes['datetime'])

    missing_datetimes = expected_set - actual_set
    extra_datetimes = actual_set - expected_set

    quantiles = np.linspace(0, 1, 100)  # 100 evenly spaced quantiles
    q_niv = imbalance_volumes['niv'].quantile(quantiles)
    q_cf  = imbalance_volumes['counterfactual_niv'].quantile(quantiles)

    plt.figure(figsize=(6,6))
    plt.plot(q_niv, q_cf, 'o', markersize=4, label='Empirical quantiles')
    plt.plot([q_niv.min(), q_niv.max()], [q_niv.min(), q_niv.max()],
            'r--', label='1:1 line')

    plt.xlabel('niv quantiles')
    plt.ylabel('counterfactual_niv quantiles')
    plt.title('Q–Q plot: niv vs counterfactual_niv')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
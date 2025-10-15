import ancillary_files.datetime_functions as datetime_functions
import isem_analysis.api_interaction as api_interaction
import isem_analysis.imbalance_volume_calculator as imbalance_volume_calculator
import pandas as pd

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
    
    print(imbalance_volumes)
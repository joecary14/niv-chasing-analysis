import pandas as pd

def calculate_imbalance_volume_by_unit(
    raw_bm_103_data: list[pd.DataFrame],
    imbalance_price_data: pd.DataFrame
) -> pd.DataFrame:
    data_rows = []
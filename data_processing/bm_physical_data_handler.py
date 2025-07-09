import pandas as pd

def get_physical_volume(
    original_data: pd.DataFrame
) -> float:
    physical_data = original_data.copy()
    physical_data.loc[:, ct.ColumnHeaders.TIME_FROM.value] = pd.to_datetime(physical_data[ct.ColumnHeaders.TIME_FROM.value])
    physical_data.loc[:, ct.ColumnHeaders.TIME_TO.value] = pd.to_datetime(physical_data[ct.ColumnHeaders.TIME_TO.value])
    physical_data.loc[:, 'duration_hours'] = (physical_data[ct.ColumnHeaders.TIME_TO.value] - physical_data[ct.ColumnHeaders.TIME_FROM.value]).dt.total_seconds() / ct.DateTime.SECONDS_PER_HOUR
    physical_data.loc[:, 'energy_MWh'] = ((physical_data[ct.ColumnHeaders.LEVEL_FROM.value] + physical_data[ct.ColumnHeaders.LEVEL_TO.value]) / 2) * physical_data['duration_hours']
    total_energy_MWh = physical_data['energy_MWh'].sum()
    
    return total_energy_MWh
import pytz
from datetime import datetime, timedelta

gb_timezone = pytz.timezone('Europe/London')

def get_settlement_dates_and_settlement_periods_per_day(
    start_date: str, 
    end_date_str: str
) -> dict[str, int]:
    full_date_list = generate_settlement_dates(start_date, end_date_str)
    dates_with_settlement_periods_per_day = get_settlement_periods_for_each_day_in_date_range(full_date_list)
    dates_with_settlement_periods_per_day = {key.strftime('%Y-%m-%d'): value 
                                                 for key, value in dates_with_settlement_periods_per_day.items()}
    return dates_with_settlement_periods_per_day

def generate_settlement_dates(start_date: str, 
                              end_date: str
) -> list[datetime]:
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD") from e

    date_list = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    
    return date_list

def get_settlement_periods_for_each_day_in_date_range(settlement_dates_inclusive: list[datetime]
) -> dict[datetime, int]:
    settlement_periods_per_day = {}
    settlement_dates_for_calculation = settlement_dates_inclusive + [
        settlement_dates_inclusive[-1] + timedelta(days = 1)]

    for i in range(len(settlement_dates_for_calculation) - 1):
        current_date = settlement_dates_for_calculation[i]
        next_date = settlement_dates_for_calculation[i+1]
        offset_now = gb_timezone.utcoffset(current_date)
        offset_next = gb_timezone.utcoffset(next_date)
        settlement_periods_in_day = 48

        if offset_now != offset_next:
            settlement_periods_in_day = (46 if offset_next > offset_now else 50)
            
        settlement_periods_per_day[current_date] = settlement_periods_in_day
    
    return settlement_periods_per_day

def add_settlement_date_to_end_of_list(settlement_dates_inclusive):
    last_settlement_date = settlement_dates_inclusive[-1]
    if type(last_settlement_date) == str:
        last_settlement_date = datetime.strptime(last_settlement_date, '%Y-%m-%d')
    additional_date = last_settlement_date + timedelta(days = 1)
    if type(settlement_dates_inclusive[0]) == str:
        additional_date = additional_date.strftime('%Y-%m-%d')
    return settlement_dates_inclusive + [additional_date]
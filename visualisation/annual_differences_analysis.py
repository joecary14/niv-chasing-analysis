import os

import pandas as pd

import ancillary_files.excel_interaction as excel_interaction
from gb_analysis.recalculate_niv import (
    get_bsc_id_to_npt_mapping,
    get_npt_imbalance_data,
    get_npt_imbalance_data_zero_metered_volume,
)


def _sum_absolute_gross_npt_positions(
    mr1b_df: pd.DataFrame,
    bsc_roles_to_npt_mapping: dict[str, bool],
    zero_metered_volume_only: bool
) -> float:
    mr1b_df = mr1b_df.copy()
    mr1b_df['Party ID'] = mr1b_df['Party ID'].map(lambda x: x.strip() if isinstance(x, str) else x)

    npt_rows = mr1b_df[mr1b_df['Party ID'].map(bsc_roles_to_npt_mapping) == True]
    # Matches the exclusion applied in recalculate_niv.get_npt_imbalance_data / _zero_metered_volume
    npt_rows = npt_rows[
        (npt_rows['Party ID'] != 'CBBLESTN') &
        (npt_rows['Party ID'] != 'OLDKENT')
    ]
    if zero_metered_volume_only:
        credited_energy_vol_column_name = 'Credited Energy Vol' if 'Credited Energy Vol' in npt_rows.columns else 'CreditedEnergyVol'
        npt_rows = npt_rows[npt_rows[credited_energy_vol_column_name] == 0]

    gross_positions = npt_rows['Energy Imbalance Vol'].abs().sum()

    return gross_positions


def summarise_npt_positions_by_month(
    mr1b_directory: str,
    bsc_roles_filepath: str,
    strict_npt: bool,
    output_filepath: str
) -> pd.DataFrame:
    """
    For every MR1B_<year>_<month>.xlsx file found in mr1b_directory, compute,
    under both the standard and zero-metered-volume-only NPT definitions:
      - the sum of absolute NPT positions taken by each party in each period
        (gross, i.e. before netting across parties)
      - the sum of absolute net NPT positions taken in each period
        (net, i.e. after netting across parties)
    and save the resulting one-row-per-month table to output_filepath.
    """
    mr1b_filepaths = excel_interaction.get_excel_filepaths(mr1b_directory)
    filepath_dict = excel_interaction.create_filepath_dict(mr1b_filepaths)
    bsc_roles_to_npt_mapping = get_bsc_id_to_npt_mapping(bsc_roles_filepath, strict_npt)

    rows = []
    for year_month in sorted(filepath_dict.keys()):
        mr1b_df = pd.read_excel(filepath_dict[year_month])
        mr1b_df = mr1b_df.map(lambda x: x.strip() if isinstance(x, str) else x)

        gross_standard = _sum_absolute_gross_npt_positions(
            mr1b_df, bsc_roles_to_npt_mapping, zero_metered_volume_only=False)
        net_standard = get_npt_imbalance_data(
            mr1b_df, bsc_roles_to_npt_mapping)['npt_total_imbalance'].abs().sum()

        gross_zero_mv = _sum_absolute_gross_npt_positions(
            mr1b_df, bsc_roles_to_npt_mapping, zero_metered_volume_only=True)
        net_zero_mv = get_npt_imbalance_data_zero_metered_volume(
            mr1b_df, bsc_roles_to_npt_mapping)['npt_total_imbalance'].abs().sum()

        rows.append({
            'Year-Month': year_month.replace('_', '-'),
            'Sum of Absolute NPT Positions by Party (Standard)': gross_standard,
            'Sum of Absolute Net NPT Position by Period (Standard)': net_standard,
            'Sum of Absolute NPT Positions by Party (Zero Metered Volume Only)': gross_zero_mv,
            'Sum of Absolute Net NPT Position by Period (Zero Metered Volume Only)': net_zero_mv,
        })
        print(f'Completed NPT position summary for {year_month}')

    summary_df = pd.DataFrame(rows)

    output_directory, output_filename = os.path.split(output_filepath)
    output_directory = output_directory or '.'
    output_filename = os.path.splitext(output_filename)[0]
    excel_interaction.dataframes_to_excel([summary_df], output_directory, output_filename, ['NPT Position Summary'])

    return summary_df

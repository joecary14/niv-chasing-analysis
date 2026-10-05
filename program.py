import asyncio

from gb_analysis.engine import run
from gb_analysis.recalculate_imbalance_cashflows import recalculate_imbalance_cashflows_from_excel
from ancillary_files.excel_interaction import aggregate_results_to_one_file


years = [2023]
months = [i for i in range(12, 13)]  # January to December
output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Withou Cobblestone and possible stack_data_handler error/AMV'
bsc_roles = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
tlms = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Winter 2022 TLMs.xlsx'
bmu_id_to_ci_mapping = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BMU to CI Mapping.xlsx'
strict_npt = True
strict_supplier = True
strict_generator = True
zero_metered_volume_only = False

async def main():
    recalculate_imbalance_cashflows_from_excel(
        bsc_roles,
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Withou Cobblestone and possible stack_data_handler error/Imbalance Prices.xlsx',
        'Sheet1',
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Data/Elexon/MR1B Excel Reports',
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Withou Cobblestone and possible stack_data_handler error'
    )
    await run(
        years,
        months,
        output_directory,
        bsc_roles,
        tlms,
        bmu_id_to_ci_mapping,
        strict_npt,
        strict_supplier,
        strict_generator,
        zero_metered_volume_only
    )
    
asyncio.run(main())
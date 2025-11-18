import asyncio

from gb_analysis.engine import run, determine_supplier_net_position

years = [2021, 2022, 2023, 2024]
months = [i for i in range(1, 13)]  # January to December
output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Zero Metered Volume Estimate'
bsc_roles = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
tlms = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Winter 2022 TLMs.xlsx'
bmu_id_to_ci_mapping = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BMU to CI Mapping.xlsx'
strict_npt = True
strict_supplier = True
strict_generator = True
zero_metered_volume_only = True

async def main():
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
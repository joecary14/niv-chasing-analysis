import asyncio

from gb_analysis.engine import run

years = [2021]
months = [3]  # January to December
output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Loose Results'
bsc_roles = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
tlms = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Winter 2022 TLMs.xlsx'
bmu_id_to_ci_mapping = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BMU to CI Mapping.xlsx'
strict_npt = True
strict_supplier = True
strict_generator = True

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
        strict_generator
    )
    
asyncio.run(main())
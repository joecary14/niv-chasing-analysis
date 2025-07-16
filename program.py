import asyncio

from gb_analysis.engine import run

years = [2024]
months = [11]
output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code'
bsc_roles = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
tlms = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Winter 2022 TLMs.xlsx'
strict_npt = False
strict_supplier = False
strict_generator = False

async def main():
    await run(
        years,
        months,
        output_directory,
        bsc_roles,
        tlms,
        strict_npt,
        strict_supplier,
        strict_generator
    )
    
asyncio.run(main())
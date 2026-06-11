import asyncio

import rnp_analysis.frequency_deviation_analysis as fda

output_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/Outlook/Investigations/P462 Mod/Code Testing'
output_filename = '2021 - Nov 2024 Analysis'
inputs_folder = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Results'
filename = 'Wind Output Analysis'
bm_units_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/BM_Units.json'
ci_by_fuel_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/Carbon Intensity by Fuel Type.xlsx'
supporting_data_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data'
imbalance_input_data_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Aggregated Results/Imbalance Results.xlsx'
system_price_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/System Prices.xlsx'
figures_directory = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Testing Publication Code/Figures'
bsc_roles_filepath = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Supporting Data/FINAL - Elexon BSC Roles.xlsx'
filename = 'SP Histograms'
url = 'https://reports.sem-o.com/documents/EF_PT_ALL_20250902_20250903_BALIMB_INDIC_20250903T144659.XML'

years = [2024]

async def main():
    await fda.plot_frequency_repsonse('2025-09-08', 40)
    
asyncio.run(main())
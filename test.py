import asyncio

from visualisation.sp_reference_price_analysis import get_sp_and_reference_price_data
from visualisation.annual_differences_analysis import summarise_npt_positions_by_month
import visualisation.plots_from_excel as plots_from_excel
import ancillary_files.excel_interaction as excel_interaction

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
months = [i for i in range(1, 13)]  # January to December

async def main():
    plots_from_excel.create_outturn_diff_qq_plots(
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Comparison with Intraday Price/Final Hour pre-GC sp_and_reference_price_2021-01-01_to_2024-12-31.xlsx',
        'SP and Reference Price',
        output_filename ='/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Figures/QQ Plot.pdf'
    )
    plots_from_excel.create_difference_bar_chart_from_raw(
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Plotting Data.xlsx',
        'Balancing Costs',
        output_path = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Figures/Balancing Costs.pdf'
    )
    plots_from_excel.create_quantile_difference_plots(
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Plotting Data.xlsx',
        'System Imbalances',
        output_path = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Figures/Imbalance Quantile Difference.pdf'
    )
    plots_from_excel.create_absolute_imbalance_difference_bar_chart(
        '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Results/Resubmission Analysis/Plotting Data.xlsx',
        'System Imbalances',
        output_path = '/Users/josephcary/Library/CloudStorage/OneDrive-Nexus365/First Year/Papers/NIV Chasing/Figures/Imbalance Volumes.pdf'
    )
    
asyncio.run(main())
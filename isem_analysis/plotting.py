import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

def plot_niv_quantiles(filepath):
    """
    Reads Excel data and plots quantiles of 'niv' vs 'counterfactual_niv' columns.
    
    Args:
        filepath (str): Path to the Excel file
    """
    # Read the Excel file
    df = pd.read_excel(filepath)
    
    # Calculate quantiles for both columns
    quantiles = np.linspace(0, 1, 101)  # 0th to 100th percentile
    niv_quantiles = df['niv'].quantile(quantiles)
    counterfactual_quantiles = df['counterfactual_niv'].quantile(quantiles)
    
    # Create the plot
    plt.figure(figsize=(8, 6))
    plt.plot(niv_quantiles, counterfactual_quantiles, 'b-', linewidth=2)
    plt.xlabel('NIV Quantiles')
    plt.ylabel('Counterfactual NIV Quantiles')
    plt.title('Q-Q Plot: NIV vs Counterfactual NIV')
    plt.grid(True, alpha=0.3)
    
    # Add diagonal reference line
    min_val = min(niv_quantiles.min(), counterfactual_quantiles.min())
    max_val = max(niv_quantiles.max(), counterfactual_quantiles.max())
    plt.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.7, label='y=x')
    plt.legend()
    
    plt.tight_layout()
    plt.show()
    
def plot_niv_kde(filepath):
    """
    Reads Excel data and plots kernel density estimates of 'niv' and 'counterfactual_niv' columns.
    
    Args:
        filepath (str): Path to the Excel file
    """
    # Read the Excel file
    df = pd.read_excel(filepath)
    
    # Create the plot
    plt.figure(figsize=(8, 6))
    
    # Plot KDE for both columns
    df['niv'].plot.density(label='NIV', alpha=0.7, linewidth=2)
    df['counterfactual_niv'].plot.density(label='Counterfactual NIV', alpha=0.7, linewidth=2)
    
    plt.xlabel('Value')
    plt.ylabel('Density')
    plt.title('Kernel Density Estimates: NIV vs Counterfactual NIV')
    plt.legend()
    plt.grid(True, alpha=0.3)
    # Calculate statistics
    niv_mean = df['niv'].mean()
    niv_median = df['niv'].median()
    cf_niv_mean = df['counterfactual_niv'].mean()
    cf_niv_median = df['counterfactual_niv'].median()

    # Create text box with statistics
    stats_text = f'NIV: μ={niv_mean:.3f}, M={niv_median:.3f}\nCounterfactual NIV: μ={cf_niv_mean:.3f}, M={cf_niv_median:.3f}'
    plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes, 
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    plt.show()
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np

def violin_plot(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()
    df_melted = df.melt(var_name='variable', value_name='value')
    plt.figure(figsize=(10, 6))
    sns.violinplot(data=df_melted, x='variable', y='value')
    plt.title('Violin Plot')
    plt.xticks(rotation=45)
    plt.tight_layout()
    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    filename = f"{output_directory}/{output_filename}"
    if os.path.exists(filename):
        output_filename = output_filename.replace('.png', f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        filename = f"{output_directory}/{output_filename}"
    plt.savefig(filename)
    plt.show()
    plt.close()

def split_violin_plot(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    if len(headers_to_plot) != 2:
        raise ValueError("Split violin plot requires exactly two columns")
    
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()
    df_melted = df.melt(var_name='variable', value_name='value')
    
    plt.figure(figsize=(8, 6))
    sns.violinplot(data=df_melted, x='variable', y='value', split=True, inner='quart')
    plt.title('Split Violin Plot')
    plt.tight_layout()
    
    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    filename = f"{output_directory}/{output_filename}"
    if os.path.exists(filename):
        output_filename = output_filename.replace('.png', f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        filename = f"{output_directory}/{output_filename}"
    
    plt.savefig(filename)
    plt.show()
    plt.close()

def plot_overlayed_histograms(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()
    for header in headers_to_plot:
        p1, p99 = np.percentile(df[header], [1, 99])
        df = df[(df[header] >= p1) & (df[header] <= p99)]
    
    plt.figure(figsize=(10, 6))
    for header in headers_to_plot:
        sns.histplot(df[header], kde=True, label=header, stat='density', element='step')
    
    plt.title('Overlayed Histograms')
    plt.xlabel('Value')
    plt.ylabel('Density')
    plt.legend()
    plt.axvline(x=0, color='black', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    plt.savefig(f"{output_directory}/{output_filename}")
    plt.show()
    plt.close()

def create_q_q_plot(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()

    if len(headers_to_plot) != 2:
        raise ValueError("Q-Q plot requires exactly two columns")

    col1, col2 = headers_to_plot
    data1 = df[col1].values
    data2 = df[col2].values

    # Sort the data to get quantiles
    data1_sorted = sorted(data1)
    data2_sorted = sorted(data2)
    
    # Create quantiles for plotting (interpolate if different lengths)
    n = min(len(data1_sorted), len(data2_sorted))
    quantiles1 = [data1_sorted[int(i * (len(data1_sorted) - 1) / (n - 1))] for i in range(n)]
    quantiles2 = [data2_sorted[int(i * (len(data2_sorted) - 1) / (n - 1))] for i in range(n)]

    plt.figure(figsize=(8, 8))
    plt.scatter(quantiles1, quantiles2, alpha=0.6)
    plt.xlabel(f'Quantiles of {col1}')
    plt.ylabel(f'Quantiles of {col2}')
    plt.title(f'Q-Q Plot: {col1} vs {col2}')

    min_val = min(min(quantiles1), min(quantiles2))
    max_val = max(max(quantiles1), max(quantiles2))
    plt.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8)

    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    filename = f"{output_directory}/{output_filename}"
    if os.path.exists(filename):
        output_filename = output_filename.replace('.png', f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        filename = f"{output_directory}/{output_filename}"
    

    plt.savefig(f"{output_directory}/{output_filename}")
    plt.show()
    plt.close()

def plot_empirical_cdf(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()
    
    plt.figure(figsize=(10, 6))
    
    for header in headers_to_plot:
        data = df[header].values
        data_sorted = sorted(data)
        n = len(data_sorted)
        y = [i/n for i in range(1, n+1)]  # Empirical CDF values
        plt.plot(data_sorted, y, label=header, linewidth=2)
    
    plt.title('Empirical Cumulative Distribution Functions')
    plt.xlabel('Value')
    plt.ylabel('Cumulative Probability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 1)
    plt.tight_layout()
    
    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    filename = f"{output_directory}/{output_filename}"
    if os.path.exists(filename):
        output_filename = output_filename.replace('.png', f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        filename = f"{output_directory}/{output_filename}"
    
    plt.savefig(filename)
    plt.show()
    plt.close()
    
def plot_difference_curve(
    input_data_filepath: str,
    headers_to_plot: list[str],
    output_directory: str,
    output_filename: str,
    x_axis_label: str,
    bins: int = 100
) -> None:
    if len(headers_to_plot) != 2:
        raise ValueError("Difference curve plot requires exactly two columns")
    
    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()
    
    col1, col2 = headers_to_plot
    data1 = df[col1].values
    data2 = df[col2].values
    p1_data1, p99_data1 = np.percentile(data1, [1, 99])
    p1_data2, p99_data2 = np.percentile(data2, [1, 99])
    mask1 = (data1 >= p1_data1) & (data1 <= p99_data1)
    mask2 = (data2 >= p1_data2) & (data2 <= p99_data2)

    data1 = data1[mask1]
    data2 = data2[mask2]
    min_val = min(data1.min(), data2.min())
    max_val = max(data1.max(), data2.max())
    bin_edges = np.linspace(min_val, max_val, bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    hist1, _ = np.histogram(data1, bins=bin_edges, density=True)
    hist2, _ = np.histogram(data2, bins=bin_edges, density=True)
    
    difference = hist1 - hist2
    
    plt.figure(figsize=(10, 6))
    plt.plot(bin_centers, difference, linewidth=2, color='blue')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.7)
    plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
    plt.fill_between(bin_centers, difference, 0, alpha=0.3, color='blue')
    
    plt.title(f'Difference Curve: {col1} - {col2}')
    plt.xlabel(f'{x_axis_label} (Bins)')
    plt.ylabel(f'Density Difference ({col1} - {col2})')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    output_directory = output_directory.rstrip('/')
    if not output_filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf', '.svg')):
        output_filename += '.png'
    
    filename = f"{output_directory}/{output_filename}"
    if os.path.exists(filename):
        output_filename = output_filename.replace('.png', f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        filename = f"{output_directory}/{output_filename}"
    
    plt.savefig(filename)
    plt.show()
    plt.close()
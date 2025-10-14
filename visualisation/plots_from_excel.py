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

def create_side_by_side_q_q_plots(
    input_data_filepath_1: str,
    headers_to_plot_1: list[str],
    input_data_filepath_2: str,
    headers_to_plot_2: list[str],
    output_directory: str,
    output_filename: str
) -> None:
    # Read and prepare data for plot 1
    df1 = pd.read_excel(input_data_filepath_1)
    df1 = df1[headers_to_plot_1].dropna()
    if len(headers_to_plot_1) != 2:
        raise ValueError("Each Q-Q plot requires exactly two columns")
    col1a, col1b = headers_to_plot_1
    data1a = df1[col1a].values
    data1b = df1[col1b].values
    n1 = min(len(data1a), len(data1b))
    q1a = np.sort(data1a)[:n1]
    q1b = np.sort(data1b)[:n1]
    # Axis limits for plot 1
    x1_p1, x1_p99 = np.percentile(q1a, [1, 99])
    y1_p1, y1_p99 = np.percentile(q1b, [1, 99])

    # Read and prepare data for plot 2
    df2 = pd.read_excel(input_data_filepath_2)
    df2 = df2[headers_to_plot_2].dropna()
    if len(headers_to_plot_2) != 2:
        raise ValueError("Each Q-Q plot requires exactly two columns")
    col2a, col2b = headers_to_plot_2
    data2a = df2[col2a].values
    data2b = df2[col2b].values
    n2 = min(len(data2a), len(data2b))
    q2a = np.sort(data2a)[:n2]
    q2b = np.sort(data2b)[:n2]
    # Axis limits for plot 2
    x2_p1, x2_p99 = np.percentile(q2a, [1, 99])
    y2_p1, y2_p99 = np.percentile(q2b, [1, 99])

    # Plot side-by-side
    fig, axes = plt.subplots(1, 2, figsize=(16, 7), constrained_layout=True)
    # Plot 1
    axes[0].scatter(q1a, q1b, alpha=0.6)
    min1, max1 = min(q1a.min(), q1b.min()), max(q1a.max(), q1b.max())
    axes[0].plot([min1, max1], [min1, max1], 'r--', alpha=0.8)
    axes[0].set_xlabel(f'Quantiles of {col1a} (£/MWh)')
    axes[0].set_ylabel(f'Quantiles of {col1b} (£/MWh)')
    axes[0].grid(True, alpha=0.3)
    axes[0].set_xlim(x1_p1, x1_p99)
    axes[0].set_ylim(y1_p1, y1_p99)

    # Plot 2
    axes[1].scatter(q2a, q2b, alpha=0.6)
    min2, max2 = min(q2a.min(), q2b.min()), max(q2a.max(), q2b.max())
    axes[1].plot([min2, max2], [min2, max2], 'r--', alpha=0.8)
    axes[1].set_xlabel(f'Quantiles of {col2a} (£, nominal)')
    axes[1].set_ylabel(f'Quantiles of {col2b} (£, nominal)')
    axes[1].grid(True, alpha=0.3)
    axes[1].set_xlim(x2_p1, x2_p99)
    axes[1].set_ylim(y2_p1, y2_p99)

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
    # Set axis limits to 1st-99th percentile range
    p1, p99 = np.percentile(np.concatenate([quantiles1, quantiles2]), [1, 99])
    plt.xlim(p1, p99)
    plt.ylim(p1, p99)

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
    # Set axis limits to 1st-99th percentile range
    all_data = pd.concat([df[header] for header in headers_to_plot])
    p1, p99 = np.percentile(all_data, [1, 99])
    
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
    plt.xlim(p1, p99)
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

    # Remove the original single-axis plot code
    # plt.figure(figsize=(10, 6))
    # plt.plot(bin_centers, difference, linewidth=2, color='blue')
    # plt.axhline(y=0, color='black', linestyle='--', alpha=0.7)
    # plt.axvline(x=0, color='red', linestyle='--', alpha=0.7)
    # plt.fill_between(bin_centers, difference, 0, alpha=0.3, color='blue')
    # 
    # plt.title(f'Difference Curve: {col1} - {col2}')
    # plt.xlabel(f'{x_axis_label} (Bins)')
    # plt.ylabel(f'Density Difference ({col1} - {col2})')
    # plt.grid(True, alpha=0.3)
    # plt.tight_layout()
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
    
def create_price_imbalance_scatter_plots(
    input_filepath: str,
    output_directory: str,
    output_filename: str
) -> None:
    df = pd.read_excel(input_filepath)
    df_factual = df[['Factual Imbalance Volume', 'Factual System Price']].dropna()
    df_counterfactual = df[['Counterfactual Imbalance Volume', 'Counterfactual System Price']].dropna()

    # Determine axis limits for consistent scaling
    all_imbalance = pd.concat([df_factual['Factual Imbalance Volume'], df_counterfactual['Counterfactual Imbalance Volume']])
    all_price = pd.concat([df_factual['Factual System Price'], df_counterfactual['Counterfactual System Price']])

    x_min, x_max = all_imbalance.min(), all_imbalance.max()
    y_min, y_max = all_price.min(), all_price.max()
    
    # Create side-by-side plots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Factual plot
    factual_positive = df_factual[df_factual['Factual Imbalance Volume'] >= 0]
    factual_negative = df_factual[df_factual['Factual Imbalance Volume'] < 0]

    ax1.scatter(factual_positive['Factual Imbalance Volume'], factual_positive['Factual System Price'], 
                c='blue', alpha=0.6, label='Imbalance ≥ 0')
    ax1.scatter(factual_negative['Factual Imbalance Volume'], factual_negative['Factual System Price'], 
                c='orange', alpha=0.6, label='Imbalance < 0')
    
    ax1.set_xlim(x_min, x_max)
    ax1.set_ylim(y_min, y_max)
    ax1.set_xlabel('Imbalance Volume')
    ax1.set_ylabel('System Price')
    ax1.set_title('Factual Data')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Counterfactual plot
    counterfactual_positive = df_counterfactual[df_counterfactual['Counterfactual Imbalance Volume'] >= 0]
    counterfactual_negative = df_counterfactual[df_counterfactual['Counterfactual Imbalance Volume'] < 0]

    ax2.scatter(counterfactual_positive['Counterfactual Imbalance Volume'], counterfactual_positive['Counterfactual System Price'], 
                c='blue', alpha=0.6, label='Imbalance ≥ 0')
    ax2.scatter(counterfactual_negative['Counterfactual Imbalance Volume'], counterfactual_negative['Counterfactual System Price'], 
                c='orange', alpha=0.6, label='Imbalance < 0')
    
    ax2.set_xlim(x_min, x_max)
    ax2.set_ylim(y_min, y_max)
    ax2.set_xlabel('Imbalance Volume')
    ax2.set_ylabel('System Price')
    ax2.set_title('Counterfactual Data')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
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
    
def plot_kernel_density_difference(
        input_data_filepath: str,
        headers_to_plot: list[str],
        output_directory: str,
        output_filename: str,
        x_axis_label: str = "Value"
    ) -> None:
    """
    Plot kernel density difference and original densities on separate y-axes.
    Data is filtered to 1st-99th percentile range.
    """
    # Ensure both y-axes have 0 at the same location
    if len(headers_to_plot) != 2:
        raise ValueError("Kernel density difference plot requires exactly two columns")

    df = pd.read_excel(input_data_filepath)
    df = df[headers_to_plot]
    df = df.dropna()

    col1, col2 = headers_to_plot
    data1 = df[col1].values
    data2 = df[col2].values

    # Filter to 1st-99th percentile for all data combined
    all_data = np.concatenate([data1, data2])
    p1, p99 = np.percentile(all_data, [1, 99])

    data1_filtered = data1[(data1 >= p1) & (data1 <= p99)]
    data2_filtered = data2[(data2 >= p1) & (data2 <= p99)]

    # Create common x-axis range
    x_min = min(data1_filtered.min(), data2_filtered.min())
    x_max = max(data1_filtered.max(), data2_filtered.max())
    x_range = np.linspace(x_min, x_max, 200)

    # Calculate kernel densities
    kde1 = stats.gaussian_kde(data1_filtered)
    kde2 = stats.gaussian_kde(data2_filtered)

    density1 = kde1(x_range)
    density2 = kde2(x_range)
    density_diff = density1 - density2

    # Create plot with dual y-axes
    with plt.rc_context({
        'font.size': 14,
        'axes.titlesize': 16,
        'axes.labelsize': 14,
        'xtick.labelsize': 12,
        'ytick.labelsize': 12,
        'legend.fontsize': 12,
        'lines.linewidth': 3,
        'axes.linewidth': 2,
        'xtick.major.width': 2,
        'ytick.major.width': 2,
        'grid.linewidth': 1.5,
        'font.weight': 'bold',
        'axes.labelweight': 'bold',
        'axes.titleweight': 'bold'
    }):
        
        fig, ax1 = plt.subplots(figsize=(12, 8))
        ax2 = ax1.twinx()

        # Plot original densities on left axis
        line1 = ax1.plot(x_range, density1, label=col1, color='blue', linewidth=2)
        line2 = ax1.plot(x_range, density2, label=col2, color='red', linewidth=2)
        ax1.set_xlabel(x_axis_label)
        ax1.set_ylabel('Kernel Density', color='black')
        ax1.grid(True, alpha=0.3)

        # Plot difference on right axis
        line3 = ax2.plot(x_range, density_diff, label=f'{col1} - {col2}', 
                            color='green', linewidth=2, linestyle='--')
        ax2.axhline(y=0, color='gray', linestyle='-', alpha=0.5)
        ax2.set_ylabel(f'Density Difference\n({col1} - {col2})', color='green')

        # Add vertical line at x=0 if it's in range
        if x_min <= 0 <= x_max:
            ax1.axvline(x=0, color='black', linestyle=':', alpha=0.7)

        # Combine legends
        lines = line1 + line2 + line3
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper left')

        ax1_ylim = ax1.get_ylim()
        ax2_ylim = ax2.get_ylim()

        # Calculate the ratio to align zeros
        if ax1_ylim[0] < 0 and ax1_ylim[1] > 0 and ax2_ylim[0] < 0 and ax2_ylim[1] > 0:
            # Both axes cross zero - align them
            ax1_ratio = abs(ax1_ylim[0]) / (ax1_ylim[1] - ax1_ylim[0])
            ax2_ratio = abs(ax2_ylim[0]) / (ax2_ylim[1] - ax2_ylim[0])
            
            if ax1_ratio > ax2_ratio:
                # Extend ax2 negative range
                new_ax2_min = -ax2_ylim[1] * ax1_ratio / (1 - ax1_ratio)
                ax2.set_ylim(new_ax2_min, ax2_ylim[1])
            else:
                # Extend ax1 negative range
                new_ax1_min = -ax1_ylim[1] * ax2_ratio / (1 - ax2_ratio)
                ax1.set_ylim(new_ax1_min, ax1_ylim[1])

        plt.tight_layout()

        # Save figure
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

import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np
from typing import Optional, Sequence, Dict

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

def _infer_columns(df: pd.DataFrame, preferred: Sequence[str]) -> Dict[str, str]:
    """Find best-matching column names in df for each preferred name (case-insensitive, substring)."""
    cols = list(df.columns)
    lookup = {}
    lowered = [c.lower() for c in cols]
    for name in preferred:
        target = name.lower()
        # exact match
        if name in cols:
            lookup[name] = name
            continue
        if target in lowered:
            lookup[name] = cols[lowered.index(target)]
            continue
        # substring match
        match = None
        for c, lc in zip(cols, lowered):
            if target in lc or lc in target:
                match = c
                break
        if match:
            lookup[name] = match
    return lookup

def create_ecdf_plot(
    excel_path: str,
    sheet_name: str,
    columns: Optional[Dict[str, str]] = None,
    output_path: str = "ecdf_niv.pdf",
    figsize=(8, 5),
    fontsize=12,
    save_svg: bool = False,
    save_eps: bool = False,
):

    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    expected = ['factual', 'AMV', 'ZMV']
    if columns is None:
        inferred = _infer_columns(df, expected)
    else:
        inferred = columns.copy()

    missing = [k for k in expected if k not in inferred]
    if missing:
        raise ValueError(
            "Missing scenario columns: "
            f"{missing}. Available: {list(df.columns)}"
        )

    s_f = pd.Series(df[inferred['factual']]).dropna().astype(float)
    s_a = pd.Series(df[inferred['AMV']]).dropna().astype(float)
    s_z = pd.Series(df[inferred['ZMV']]).dropna().astype(float)

    def ecdf_values(arr: np.ndarray):
        n = arr.size
        if n == 0:
            return np.array([]), np.array([])
        x = np.sort(arr)
        y = np.arange(1, n + 1) / n
        return x, y

    xf, yf = ecdf_values(s_f.values)
    xa, ya = ecdf_values(s_a.values)
    xz, yz = ecdf_values(s_z.values)

    # ------------------------------------------------------------
    # Compute global x-limits (1st–99th percentile across all scenarios)
    # ------------------------------------------------------------
    combined = np.concatenate([s_f.values, s_a.values, s_z.values])
    x_lo = np.percentile(combined, 1)
    x_hi = np.percentile(combined, 99)
    x_lo = -1000
    x_hi = 1000

    # ------------------------------------------------------------
    # Plot
    # ------------------------------------------------------------
    plt.close('all')
    fig, ax = plt.subplots(figsize=figsize)
    plt.rcParams.update({'font.size': fontsize})

    # Plot each ECDF only within the restricted x-range
    def plot_trimmed(x, y, label, **kwargs):
        if x.size == 0:
            return
        mask = (x >= x_lo) & (x <= x_hi)
        if mask.sum() == 0:
            return
        ax.step(x[mask], y[mask], where='post', label=label, **kwargs)

    plot_trimmed(xf, yf, 'Factual', linewidth=1.5)
    plot_trimmed(xa, ya, 'AMV (no NPT)', linewidth=1.25, linestyle='--')
    plot_trimmed(xz, yz, 'ZMV (no NPT)', linewidth=1.25, linestyle=':')

    ax.set_xlabel('Net Imbalance Volume (MWh)')
    ax.set_ylabel('Empirical Cumulative Probability')
    ax.set_xlim(x_lo, x_hi)
    ax.set_ylim(-0.02, 1.02)
    ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.legend(frameon=False)
    ax.ticklabel_format(axis='x', style='sci', scilimits=(-3, 4))
    ax.axvline(0, color='black', linestyle='--', linewidth=1)
    plt.tight_layout()

    out_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    os.makedirs(out_dir, exist_ok=True)

    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    base, ext = os.path.splitext(output_path)
    if save_svg:
        plt.savefig(base + '.svg', format='svg', bbox_inches='tight')
    if save_eps:
        plt.savefig(base + '.eps', format='eps', bbox_inches='tight')
    plt.close(fig)

    return os.path.abspath(output_path)

def create_difference_bar_chart_from_raw(
    excel_path: str,
    sheet_name: str,
    year_col: str = "Year",
    factual_col: str = "Factual",
    amv_col: str = "AMV",
    zmv_col: str = "ZMV",
    output_path: str = "niv_differences_by_year.pdf",
    figsize=(7, 4),
    fontsize=12,
    aggfunc="sum",   # or "mean"
    save_svg: bool = False,
    save_eps: bool = False,
):
    """
    Read raw (row-level) Excel data, aggregate by `year_col`, compute AMV–Factual and ZMV–Factual
    per year, and produce a grouped bar chart saved as a vector file suitable for Overleaf.

    Parameters
    ----------
    excel_path : str
        Path to the Excel file.
    sheet_name : str
        Sheet name (or index).
    year_col, factual_col, amv_col, zmv_col : str
        Column names in the sheet.
    output_path : str
        Output file path (PDF by default).
    aggfunc : {"sum","mean"} or callable
        How to aggregate rows within each year.
    """
    # Read data
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    if year_col not in df.columns:
        raise KeyError(f"Year column '{year_col}' not found in sheet. Available columns: {list(df.columns)}")

    # Ensure numeric columns exist
    for c in (factual_col, amv_col, zmv_col):
        if c not in df.columns:
            raise KeyError(f"Required column '{c}' not found in sheet. Available columns: {list(df.columns)}")
        # coerce to numeric, allow errors -> NaN
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalize year column (if datetime-like, extract year)
    if pd.api.types.is_datetime64_any_dtype(df[year_col]):
        df["_year_norm"] = df[year_col].dt.year
    else:
        # Try integer conversion (handles strings like "2021")
        df["_year_norm"] = pd.to_numeric(df[year_col], errors="coerce").astype('Int64')

    if df["_year_norm"].isna().any():
        raise ValueError("Some Year values could not be parsed as years. Check the Year column.")

    # Aggregate by year
    if aggfunc == "sum":
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].sum(min_count=1)
    elif aggfunc == "mean":
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].mean()
    else:
        # allow callable
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].agg(aggfunc)

    agg = agg.sort_index()
    years = list(agg.index.astype(int))

    # Compute differences
    diff_amv = (agg[amv_col] - agg[factual_col])/1000000  # Convert to TWh
    diff_zmv = (agg[zmv_col] - agg[factual_col])/1000000  # Convert to TWh

    # Plot
    plt.close('all')
    fig, ax = plt.subplots(figsize=figsize)
    plt.rcParams.update({'font.size': fontsize})

    x = list(range(len(years)))
    bar_width = 0.35

    ax.bar([i - bar_width/2 for i in x], diff_amv, width=bar_width, label="AMV – Factual")
    ax.bar([i + bar_width/2 for i in x], diff_zmv, width=bar_width, label="ZMV – Factual")

    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.set_xlabel("Year")
    ax.set_ylabel("Difference in BM costs (£m, nominal)")
    ax.ticklabel_format(axis='y', style='plain')
    ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.legend(frameon=False)
    plt.tight_layout()

    # Save vector output
    out_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    os.makedirs(out_dir, exist_ok=True)

    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    base, _ = os.path.splitext(output_path)
    if save_svg:
        plt.savefig(base + '.svg', format='svg', bbox_inches='tight')
    if save_eps:
        plt.savefig(base + '.eps', format='eps', bbox_inches='tight')
    plt.close(fig)

    return os.path.abspath(output_path)

def create_qq_plots(
    excel_path: str,
    sheet_name: str,
    factual_col: str = "Price_Factual",
    amv_col: str = "Price_AMV",
    zmv_col: str = "Price_ZMV",
    output_path: str = "qq_prices.pdf",
    figsize=(10, 5),
    fontsize=12,
    save_svg=False,
    save_eps=False,
):
    """
    Create a two-panel Q–Q figure:
      • Top panel: Factual vs AMV
      • Bottom panel: Factual vs ZMV
    Both panels share the same factual x-axis.
    """

    # Read data
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Extract and drop missing
    f = pd.to_numeric(df[factual_col], errors="coerce").dropna()
    a = pd.to_numeric(df[amv_col], errors="coerce").dropna()
    z = pd.to_numeric(df[zmv_col], errors="coerce").dropna()

    # Align lengths via quantiles for Q-Q comparison
    def qq_pair(base, other, n=200):
        qs = np.linspace(0, 1, n)
        return (
            np.quantile(base, qs),
            np.quantile(other, qs)
        )

    xf_amv, ya_amv = qq_pair(f, a)
    xf_zmv, ya_zmv = qq_pair(f, z)

    # Plot
    plt.close('all')
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharex=True)
    plt.rcParams.update({'font.size': fontsize})
    all_prices = pd.concat([f, a, z])
    x_lo = np.percentile(all_prices, 1)
    x_hi = np.percentile(all_prices, 99)

    # Apply axis limits to both subplots

    # --- AMV subplot ---
    ax1 = axes[0]
    ax1.scatter(xf_amv, ya_amv, s=12)
    ax1.plot([xf_amv.min(), xf_amv.max()],
             [xf_amv.min(), xf_amv.max()],
             color='black', linewidth=1)
    ax1.set_xlim(x_lo, x_hi)
    ax1.set_ylim(x_lo, x_hi)
    ax1.set_xlabel("Factual price (£/MWh)")
    ax1.set_ylabel("AMV price (£/MWh)")
    ax1.grid(True, linestyle='--', linewidth=0.4, alpha=0.7)

    # --- ZMV subplot ---
    ax2 = axes[1]
    ax2.scatter(xf_zmv, ya_zmv, s=12)
    ax2.plot([xf_zmv.min(), xf_zmv.max()],
             [xf_zmv.min(), xf_zmv.max()],
             color='black', linewidth=1)
    ax2.set_xlabel("Factual price (£/MWh)")
    ax2.set_ylabel("ZMV price (£/MWh)")
    ax2.set_xlim(x_lo, x_hi)
    ax2.set_ylim(x_lo, x_hi)
    ax2.grid(True, linestyle='--', linewidth=0.4, alpha=0.7)

    plt.tight_layout()

    # Save vector
    out_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    os.makedirs(out_dir, exist_ok=True)

    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    base, _ = os.path.splitext(output_path)
    if save_svg:
        plt.savefig(base + '.svg', format='svg', bbox_inches='tight')
    if save_eps:
        plt.savefig(base + '.eps', format='eps', bbox_inches='tight')

    plt.close(fig)
    return os.path.abspath(output_path)

def create_boxplots(
    excel_path: str,
    sheet_name: str,
    factual_col: str = "Price_Factual",
    amv_col: str = "Price_AMV",
    zmv_col: str = "Price_ZMV",
    output_path: str = "box_prices.pdf",
    figsize=(10, 5),
    fontsize=12,
    save_svg=False,
    save_eps=False,
):
    """
    Adapted from your Q-Q function: produces two side-by-side box-and-whisker plots.
      • Left:  Factual vs AMV
      • Right: Factual vs ZMV
    The vertical axis (price) is limited to the 1st–99th percentiles across all three series.
    """

    # Read data
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Extract and drop missing
    f = pd.to_numeric(df[factual_col], errors="coerce").dropna()
    a = pd.to_numeric(df[amv_col], errors="coerce").dropna()
    z = pd.to_numeric(df[zmv_col], errors="coerce").dropna()

    # Compute common 1st–99th percentile window
    all_prices = pd.concat([f, a, z])
    x_lo = np.percentile(all_prices, 1)
    x_hi = np.percentile(all_prices, 99)

    # Prepare figure: two subplots, share y-axis
    plt.close('all')
    fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)
    plt.rcParams.update({'font.size': fontsize})

    # --- Left: Factual vs AMV ---
    ax1 = axes[0]
    box_data_amv = [f.values, a.values]
    b1 = ax1.boxplot(
        box_data_amv,
        positions=[1, 2],
        widths=0.6,
        patch_artist=True,
        showfliers=True,
        medianprops=dict(color='black'),
    )
    # optional styling: light fill
    for patch, color in zip(b1['boxes'], ['#c6dbef', '#9ecae1']):
        patch.set_facecolor(color)
    ax1.set_xticks([1, 2])
    ax1.set_xticklabels(['Factual', 'AMV'])
    ax1.set_ylabel('Price')
    ax1.set_title('Factual vs AMV')
    ax1.grid(axis='y', linestyle='--', linewidth=0.4, alpha=0.7)
    ax1.set_ylim(x_lo, x_hi)

    # --- Right: Factual vs ZMV ---
    ax2 = axes[1]
    box_data_zmv = [f.values, z.values]
    b2 = ax2.boxplot(
        box_data_zmv,
        positions=[1, 2],
        widths=0.6,
        patch_artist=True,
        showfliers=True,
        medianprops=dict(color='black'),
    )
    for patch, color in zip(b2['boxes'], ['#fde0dd', '#fa9fb5']):
        patch.set_facecolor(color)
    ax2.set_xticks([1, 2])
    ax2.set_xticklabels(['Factual', 'ZMV'])
    ax2.set_title('Factual vs ZMV')
    ax2.grid(axis='y', linestyle='--', linewidth=0.4, alpha=0.7)
    ax2.set_ylim(x_lo, x_hi)

    plt.tight_layout()

    # Save vector output
    out_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    os.makedirs(out_dir, exist_ok=True)

    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    base, _ = os.path.splitext(output_path)
    if save_svg:
        plt.savefig(base + '.svg', format='svg', bbox_inches='tight')
    if save_eps:
        plt.savefig(base + '.eps', format='eps', bbox_inches='tight')

    plt.close(fig)
    return os.path.abspath(output_path)
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np
from typing import Optional, Sequence, Dict

MM_PER_INCH = 25.4
ELSEVIER_DOUBLE_COLUMN_WIDTH_MM = 190
ELSEVIER_ECDF_FIGURE_HEIGHT_MM = 110
ELSEVIER_QQ_FIGURE_HEIGHT_MM = 100

# Fixed categorical colours for the Factual/AMV/ZMV scenarios, held consistent
# across every chart that shows them together (validated colourblind-safe triad).
SCENARIO_COLORS = {
    'AMV': '#2a78d6',
    'ZMV': '#eb6834',
    'Factual': '#1baf7a',
}

def _expand_ylim_until_legend_clears_bars(ax, legend, fig, max_iterations=12, step_fraction=0.08):
    """Grow the y-axis top until the legend's rendered bbox no longer overlaps any bar."""
    for _ in range(max_iterations):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        legend_bbox = legend.get_window_extent(renderer)
        overlaps = any(
            legend_bbox.overlaps(patch.get_window_extent(renderer))
            for patch in ax.patches
        )
        if not overlaps:
            return
        y_lo, y_hi = ax.get_ylim()
        ax.set_ylim(y_lo, y_hi + (y_hi - y_lo) * step_fraction)

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
    figsize=(ELSEVIER_DOUBLE_COLUMN_WIDTH_MM / MM_PER_INCH, ELSEVIER_ECDF_FIGURE_HEIGHT_MM / MM_PER_INCH),
    fontsize=9,
    restrict_to_1st_99th_percentile: bool = False,
    axis_margin_fraction: float = 0.05,
    save_svg: bool = False,
    save_eps: bool = False,
):
    """
    Plot the full empirical CDF of net imbalance volume for the Factual, AMV,
    and ZMV scenarios. Each step curve is built from every data point; the
    x-axis view is then restricted separately, so no data is dropped from the
    underlying curves. When restrict_to_1st_99th_percentile is True (default),
    the view is limited to the combined 1st-99th percentile range (padded by
    axis_margin_fraction); when False, the full data range is shown (with the
    same padding). Saved as a PDF sized for an Elsevier double-column figure.
    """
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
    # View window only - the full curves above still contain every point.
    # ------------------------------------------------------------
    combined = np.concatenate([s_f.values, s_a.values, s_z.values])
    if restrict_to_1st_99th_percentile:
        x_lo, x_hi = np.percentile(combined, [1, 99])
    else:
        x_lo, x_hi = combined.min(), combined.max()
    margin = (x_hi - x_lo) * axis_margin_fraction
    x_lo, x_hi = x_lo - margin, x_hi + margin

    # ------------------------------------------------------------
    # Plot
    # ------------------------------------------------------------
    plt.close('all')
    fig, ax = plt.subplots(figsize=figsize)
    plt.rcParams.update({'font.size': fontsize})

    if xf.size:
        ax.step(xf, yf, where='post', label='Factual', linewidth=1.5)
    if xa.size:
        ax.step(xa, ya, where='post', label='AMV (no NPT)', linewidth=1.25, linestyle='--')
    if xz.size:
        ax.step(xz, yz, where='post', label='ZMV (no NPT)', linewidth=1.25, linestyle=':')

    ax.set_xlabel('Net Imbalance Volume (MWh)')
    ax.set_ylabel('Empirical Cumulative Probability')
    ax.set_xlim(x_lo, x_hi)
    ax.set_ylim(-0.02, 1.02)
    ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.legend(frameon=False)
    ax.ticklabel_format(axis='x', style='sci', scilimits=(-3, 4))
    ax.axvline(0, color='black', linestyle='--', linewidth=1)
    plt.tight_layout()

    output_path = os.path.splitext(output_path)[0] + '.pdf'
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

def create_quantile_difference_plots(
    excel_path: str,
    sheet_name: str,
    columns: Optional[Dict[str, str]] = None,
    output_path: str = "quantile_differences.pdf",
    figsize=(ELSEVIER_DOUBLE_COLUMN_WIDTH_MM / MM_PER_INCH, ELSEVIER_QQ_FIGURE_HEIGHT_MM / MM_PER_INCH),
    fontsize=9,
    n_quantiles: int = 200,
    restrict_to_1st_99th_percentile: bool = True,
    axis_margin_fraction: float = 0.05,
    save_svg: bool = False,
    save_eps: bool = False,
) -> str:
    """
    Analogous to create_ecdf_plot: reads the same Factual/AMV/ZMV columns from
    the given worksheet, but instead of the CDFs, plots two quantile-difference
    (shift function) curves side by side in one figure:
      (a) AMV - Factual
      (b) ZMV - Factual
    each showing quantile(scenario) - quantile(Factual) against the quantile
    level. Quantiles are computed over a fine grid; when
    restrict_to_1st_99th_percentile is True (default) the view (and the
    plotted curve) is limited to the 1st-99th quantile level, padded by
    axis_margin_fraction, since the 0th/100th percentile is just the sample
    min/max and tends to be a noisy outlier.
    Saved as a PDF sized for an Elsevier double-column figure.
    """
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

    quantile_levels = np.linspace(0, 1, n_quantiles)
    q_factual = np.quantile(s_f, quantile_levels)
    q_amv = np.quantile(s_a, quantile_levels)
    q_zmv = np.quantile(s_z, quantile_levels)

    diff_amv = q_amv - q_factual
    diff_zmv = q_zmv - q_factual
    percentile_levels = quantile_levels * 100

    if restrict_to_1st_99th_percentile:
        x_lo, x_hi = 1, 99
    else:
        x_lo, x_hi = 0, 100
    margin = (x_hi - x_lo) * axis_margin_fraction
    mask = (percentile_levels >= x_lo) & (percentile_levels <= x_hi)

    plt.close('all')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize, constrained_layout=True)
    plt.rcParams.update({'font.size': fontsize})

    for ax, diff_values, scenario_label, color, panel_title in (
        (ax1, diff_amv, 'AMV', SCENARIO_COLORS['AMV'], '(a)'),
        (ax2, diff_zmv, 'ZMV', SCENARIO_COLORS['ZMV'], '(b)'),
    ):
        ax.plot(percentile_levels[mask], diff_values[mask], color=color, linewidth=1.5)
        ax.axhline(0, color='black', linestyle='--', linewidth=1)
        ax.set_xlim(x_lo - margin, x_hi + margin)
        ax.set_xlabel('Quantile (%)')
        ax.set_ylabel(f'{scenario_label} − Factual Quantile Difference (MWh)')
        ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
        ax.set_title(panel_title, loc='left', fontweight='bold')

    output_path = os.path.splitext(output_path)[0] + '.pdf'
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

def create_difference_bar_chart_from_raw(
    excel_path: str,
    sheet_name: str,
    year_col: str = "Year",
    factual_col: str = "Factual",
    amv_col: str = "AMV",
    zmv_col: str = "ZMV",
    output_path: str = "niv_differences_by_year.pdf",
    figsize=(ELSEVIER_DOUBLE_COLUMN_WIDTH_MM / MM_PER_INCH, ELSEVIER_QQ_FIGURE_HEIGHT_MM / MM_PER_INCH),
    fontsize=9,
    aggfunc="sum",   # or "mean"
    save_svg: bool = False,
    save_eps: bool = False,
) -> str:
    """
    Read raw (row-level) Excel data, aggregate by year_col, and plot a grouped
    bar chart of (AMV - Factual) and (ZMV - Factual) BM cost by year (each
    scenario column aggregated via aggfunc within the year, then differenced).
    AMV/ZMV use a fixed colour each, consistent with
    create_absolute_imbalance_difference_bar_chart. Saved as a PDF sized for
    an Elsevier single-column figure.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    if year_col not in df.columns:
        raise KeyError(f"Year column '{year_col}' not found in sheet. Available columns: {list(df.columns)}")

    for c in (factual_col, amv_col, zmv_col):
        if c not in df.columns:
            raise KeyError(f"Required column '{c}' not found in sheet. Available columns: {list(df.columns)}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if pd.api.types.is_datetime64_any_dtype(df[year_col]):
        df["_year_norm"] = df[year_col].dt.year
    else:
        df["_year_norm"] = pd.to_numeric(df[year_col], errors="coerce").astype('Int64')

    if df["_year_norm"].isna().any():
        raise ValueError("Some Year values could not be parsed as years. Check the Year column.")

    if aggfunc == "sum":
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].sum(min_count=1)
    elif aggfunc == "mean":
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].mean()
    else:
        agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].agg(aggfunc)
    agg = agg.sort_index()
    years = list(agg.index.astype(int))
    x = list(range(len(years)))

    diff_amv = (agg[amv_col] - agg[factual_col]) / 1_000_000  # £ -> £m
    diff_zmv = (agg[zmv_col] - agg[factual_col]) / 1_000_000  # £ -> £m

    plt.close('all')
    fig, ax = plt.subplots(figsize=figsize, constrained_layout=True)
    plt.rcParams.update({'font.size': fontsize})

    bar_width = 0.35
    ax.bar([i - bar_width / 2 for i in x], diff_amv, width=bar_width,
           label="AMV – Factual", color=SCENARIO_COLORS['AMV'])
    ax.bar([i + bar_width / 2 for i in x], diff_zmv, width=bar_width,
           label="ZMV – Factual", color=SCENARIO_COLORS['ZMV'])

    ax.axhline(0, color='black', linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.set_xlabel("Year")
    ax.set_ylabel("Difference in BM Costs\n(£m, nominal)")
    ax.ticklabel_format(axis='y', style='plain')
    ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    legend = ax.legend(frameon=False, loc='upper left')
    _expand_ylim_until_legend_clears_bars(ax, legend, fig)

    output_path = os.path.splitext(output_path)[0] + '.pdf'
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

def create_absolute_imbalance_difference_bar_chart(
    excel_path: str,
    sheet_name: str,
    year_col: str = "Year",
    factual_col: str = "Factual",
    amv_col: str = "AMV",
    zmv_col: str = "ZMV",
    output_path: str = "absolute_imbalance_differences_by_year.pdf",
    figsize=(ELSEVIER_DOUBLE_COLUMN_WIDTH_MM / MM_PER_INCH, ELSEVIER_QQ_FIGURE_HEIGHT_MM / MM_PER_INCH),
    fontsize=9,
    save_svg: bool = False,
    save_eps: bool = False,
) -> str:
    """
    Read raw (row-level) Excel data, aggregate by year_col, and plot a two-panel
    figure:
      (a) grouped bar chart of (AMV - Factual) and (ZMV - Factual) total absolute
          imbalance volume by year (each scenario column summed as absolute
          values within the year, then differenced)
      (b) grouped bar chart of the average Factual/AMV/ZMV imbalance volume per
          settlement period, by year
    Factual/AMV/ZMV use a fixed colour each, consistent across both panels.
    Saved as a PDF sized for an Elsevier double-column figure.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    if year_col not in df.columns:
        raise KeyError(f"Year column '{year_col}' not found in sheet. Available columns: {list(df.columns)}")

    for c in (factual_col, amv_col, zmv_col):
        if c not in df.columns:
            raise KeyError(f"Required column '{c}' not found in sheet. Available columns: {list(df.columns)}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if pd.api.types.is_datetime64_any_dtype(df[year_col]):
        df["_year_norm"] = df[year_col].dt.year
    else:
        df["_year_norm"] = pd.to_numeric(df[year_col], errors="coerce").astype('Int64')

    if df["_year_norm"].isna().any():
        raise ValueError("Some Year values could not be parsed as years. Check the Year column.")

    abs_values = df[[factual_col, amv_col, zmv_col]].abs()
    abs_agg = abs_values.groupby(df["_year_norm"]).sum(min_count=1).sort_index()
    years = list(abs_agg.index.astype(int))
    x = list(range(len(years)))

    diff_amv = (abs_agg[amv_col] - abs_agg[factual_col]) / 1_000_000  # MWh -> TWh
    diff_zmv = (abs_agg[zmv_col] - abs_agg[factual_col]) / 1_000_000  # MWh -> TWh

    mean_agg = df.groupby("_year_norm")[[factual_col, amv_col, zmv_col]].mean().sort_index()

    plt.close('all')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize, constrained_layout=True)
    plt.rcParams.update({'font.size': fontsize})

    # (a) Difference in total absolute imbalance volume by year
    bar_width_a = 0.35
    ax1.bar([i - bar_width_a / 2 for i in x], diff_amv, width=bar_width_a,
            label="AMV – Factual", color=SCENARIO_COLORS['AMV'])
    ax1.bar([i + bar_width_a / 2 for i in x], diff_zmv, width=bar_width_a,
            label="ZMV – Factual", color=SCENARIO_COLORS['ZMV'])

    ax1.axhline(0, color='black', linewidth=0.8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(years)
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Difference in Total Absolute\nImbalance Volume (TWh)")
    ax1.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    legend1 = ax1.legend(frameon=False, loc='upper left')
    ax1.set_title('(a)', loc='left', fontweight='bold')
    _expand_ylim_until_legend_clears_bars(ax1, legend1, fig)

    # (b) Average imbalance volume per settlement period, by year
    bar_width_b = 0.25
    ax2.bar([i - bar_width_b for i in x], mean_agg[factual_col], width=bar_width_b,
            label="Factual", color=SCENARIO_COLORS['Factual'])
    ax2.bar(x, mean_agg[amv_col], width=bar_width_b,
            label="AMV", color=SCENARIO_COLORS['AMV'])
    ax2.bar([i + bar_width_b for i in x], mean_agg[zmv_col], width=bar_width_b,
            label="ZMV", color=SCENARIO_COLORS['ZMV'])

    ax2.axhline(0, color='black', linewidth=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(years)
    ax2.set_xlabel("Year")
    ax2.set_ylabel("Average Period Imbalance Volume (MWh)")
    ax2.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
    legend2 = ax2.legend(frameon=False, loc='upper left')
    ax2.set_title('(b)', loc='left', fontweight='bold')
    _expand_ylim_until_legend_clears_bars(ax2, legend2, fig)

    output_path = os.path.splitext(output_path)[0] + '.pdf'
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

def create_outturn_diff_qq_plots(
    excel_path: str,
    sheet_name: str,
    outturn_col: str = "Outturn Diff",
    amv_col: str = "AMV Diff",
    zmv_col: str = "ZMV Diff",
    output_filename: Optional[str] = None,
    figsize: Optional[tuple] = None,
    fontsize: int = 9,
    n_quantiles: int = 200,
    restrict_to_1st_99th_percentile: bool = True,
    axis_margin_fraction: float = 0.05,
) -> str:
    """
    Create a publication-ready two-panel Q-Q figure, sized to an Elsevier
    (elsarticle) double-column figure width:
      • Left panel: Factual vs AMV System Price - Intraday Price
      • Right panel: Factual vs ZMV System Price - Intraday Price
    Quantiles are computed over a fine grid to preserve distribution shape. When
    restrict_to_1st_99th_percentile is True (default), each axis is limited to its
    own 1st-99th percentile range so outliers don't compress the plot; when False,
    the full quantile range is shown. Either way, axis limits are padded by
    axis_margin_fraction so the boundary points aren't drawn right on the edge.
    Saved as a PDF alongside the input Excel file.
    """
    if figsize is None:
        figsize = (ELSEVIER_DOUBLE_COLUMN_WIDTH_MM / MM_PER_INCH, ELSEVIER_QQ_FIGURE_HEIGHT_MM / MM_PER_INCH)

    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    quantile_levels = np.linspace(0, 1, n_quantiles)

    def qq_pair(col_a: str, col_b: str):
        a = pd.to_numeric(df[col_a], errors="coerce").dropna()
        b = pd.to_numeric(df[col_b], errors="coerce").dropna()
        return np.quantile(a, quantile_levels), np.quantile(b, quantile_levels)

    def axis_limits(values: np.ndarray) -> tuple[float, float]:
        if restrict_to_1st_99th_percentile:
            lo, hi = np.percentile(values, [1, 99])
        else:
            lo, hi = values.min(), values.max()
        margin = (hi - lo) * axis_margin_fraction
        return lo - margin, hi + margin

    x_label = 'Quantiles of Factual System Price\n- Intraday Price (£/MWh)'
    y_label_template = 'Quantiles of {} System Price\n- Intraday Price (£/MWh)'

    x_amv, y_amv = qq_pair(outturn_col, amv_col)
    x_zmv, y_zmv = qq_pair(outturn_col, zmv_col)

    plt.close('all')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize, constrained_layout=True)
    plt.rcParams.update({'font.size': fontsize})

    ax1.scatter(x_amv, y_amv, s=12, color=SCENARIO_COLORS['AMV'])
    x1_lo, x1_hi = axis_limits(x_amv)
    y1_lo, y1_hi = axis_limits(y_amv)
    lims1 = [min(x1_lo, y1_lo), max(x1_hi, y1_hi)]
    ax1.plot(lims1, lims1, color='black', linewidth=1)
    ax1.set_xlim(x1_lo, x1_hi)
    ax1.set_ylim(y1_lo, y1_hi)
    ax1.set_xlabel(x_label)
    ax1.set_ylabel(y_label_template.format('AMV'))
    ax1.grid(True, linestyle='--', linewidth=0.4, alpha=0.7)

    ax2.scatter(x_zmv, y_zmv, s=12, color=SCENARIO_COLORS['ZMV'])
    x2_lo, x2_hi = axis_limits(x_zmv)
    y2_lo, y2_hi = axis_limits(y_zmv)
    lims2 = [min(x2_lo, y2_lo), max(x2_hi, y2_hi)]
    ax2.plot(lims2, lims2, color='black', linewidth=1)
    ax2.set_xlim(x2_lo, x2_hi)
    ax2.set_ylim(y2_lo, y2_hi)
    ax2.set_xlabel(x_label)
    ax2.set_ylabel(y_label_template.format('ZMV'))
    ax2.grid(True, linestyle='--', linewidth=0.4, alpha=0.7)

    output_directory = os.path.dirname(os.path.abspath(excel_path))
    if output_filename is None:
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        output_filename = f'{base_name}_outturn_vs_amv_zmv_qq_plots.pdf'
    if not output_filename.lower().endswith('.pdf'):
        output_filename = os.path.splitext(output_filename)[0] + '.pdf'

    output_filepath = os.path.join(output_directory, output_filename)
    if os.path.exists(output_filepath):
        stem, ext = os.path.splitext(output_filename)
        output_filename = f"{stem}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}{ext}"
        output_filepath = os.path.join(output_directory, output_filename)

    plt.savefig(output_filepath, format='pdf', bbox_inches='tight')
    plt.close(fig)

    return output_filepath

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
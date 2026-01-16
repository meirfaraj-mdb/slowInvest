from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from concurrent.futures.process import ProcessPoolExecutor

from matplotlib import pyplot as plt
import matplotlib
import logging
import pandas as pd
from datetime import datetime
graph_logging = logging.getLogger("graphs")

def plot_stats(config, df, value_col, title, ylabel, xlabel='Time', output_file=None, columns=['namespace']):

    if df.empty:
        return []
    if config.GENERATE_PNG:
        if matplotlib.get_backend()!="cairo":
            matplotlib.use("cairo")
    elif matplotlib.get_backend()!="svg":
        matplotlib.use("svg")

    # Create a pivot table with hour and namespace.
    pivot_df = df.pivot_table(index='hour', columns=columns, values=value_col, aggfunc='sum', fill_value=0)

    # Find the total for each namespace and select the top 20 with the highest totals.
    total_sums = pivot_df.sum().sort_values(ascending=False)
    top_20_namespaces = total_sums.head(20).index

    # Filter the dataframe to include only the top 20 namespaces.
    pivot_df_top = pivot_df[top_20_namespaces]

    # Plot the data, using the 'tab20' colormap.
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df_top.plot(kind='line', ax=ax, colormap='tab20')  # Use 'tab20' for a qualitative colormap.

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title='Namespace', loc='upper left', bbox_to_anchor=(1, 1))
    plt.xticks(rotation=45)
    plt.tight_layout()

    file_paths = []
    if output_file:
        # Replace colons with underscores in the file name.
        output_file = output_file.replace(':', '_')

        if config.GENERATE_PNG:
            png_file_path = f"{output_file}.png"
            plt.savefig(png_file_path, bbox_inches='tight')
            graph_logging.info(f"Graph saved to {png_file_path}")
            file_paths.append(png_file_path)

        svg_file_path = f"{output_file}.svg"
        plt.savefig(svg_file_path, format="svg")
        graph_logging.info(f"Graph saved to {svg_file_path}")
        file_paths.append(svg_file_path)

    plt.close(fig)
    return file_paths



def parallel_plot_tasks(plot_args_list):
    """Run multiple plot tasks in parallel and collect their results."""
    results = []
    with ProcessPoolExecutor() as executor:
        future_to_args = {executor.submit(plot_stats, *args): args for args in plot_args_list}
        for future in future_to_args:
            try:
                result = future.result()
                results.append((future_to_args[future], result))
            except Exception as e:
                print(f"An error occurred while processing: {e}")
        executor.shutdown()
    return results

def aggregateForGraph(df,groupByConditions):
    for cond in groupByConditions:
        if cond != 'hour':
            df[cond] = df[cond].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    return df.groupby(groupByConditions).agg(
        slow_query=('slow_query', 'sum'),
        total_duration=('durationMillis_total', 'sum'),
        writeConflicts=('writeConflicts_total', 'sum'),
        has_sort_stage=('has_sort_stage', 'sum'),
        sum_skip=('skip_total', 'sum'),
        query_targeting=('query_targeting_max', 'max')).reset_index()


def createGraphBy(config, result, groupByCondition, prefix, all_plot_args):
    if result["groupByCommandShape"].get("hours",None) is None or result["groupByCommandShape"]["hours"].empty:
        return

    df = aggregateForGraph(result["groupByCommandShape"]["hours"], ['hour', groupByCondition])
    df_plan_summary = aggregateForGraph(result["groupByCommandShape"]["hours"], ['hour', groupByCondition, 'plan_summary'])
    all_plot_args.extend([
        (
            config,
            df,
            'slow_query',
            f"Number of Slow Queries per Hour per {groupByCondition}",
            'Number of Slow Queries',
            'Time',
            f"{prefix}_slow_queries_per_hour_and_{groupByCondition}",
            [groupByCondition],
        ),
        (
            config,
            df,
            'total_duration',
            f"Total Duration of Slow Queries per Hour per {groupByCondition}",
            'Total Duration (ms)',
            'Time',
            f"{prefix}_total_duration_per_hour_and_{groupByCondition}",
            [groupByCondition],
        ),
        (
            config,
            df_plan_summary[df_plan_summary['plan_summary'].str.contains('COLLSCAN')],
            'slow_query',
            f"COLLSCAN Count per Hour per {groupByCondition}",
            'COLLSCAN Count',
            'Time',
            f"{prefix}_COLLSCAN_per_hour_and_{groupByCondition}",
            [groupByCondition, 'plan_summary'],
        ),
        (
            config,
            df,
            "has_sort_stage",
            f"Has Sort Stage Count per Hour per {groupByCondition}",
            "Has Sort Stage Count",
            'Time',
            f"{prefix}_has_sort_stage_per_hour",
            [groupByCondition],
        ),
        (
            config,
            df,
            'writeConflicts',
            f"Write Conflicts Count per Hour per {groupByCondition}",
            'Write Conflicts Count',
            'Time',
            f"{prefix}_writeConflicts_per_hour_and_{groupByCondition}",
            [groupByCondition],
        ),
        (
            config,
            df,
            'query_targeting',
            f"Query Targeting per Hour per {groupByCondition}",
            'Query Targeting',
            'Time',
            f"{prefix}_query_targeting_per_hour_and_{groupByCondition}",
            [groupByCondition],
        ),
        (
            config,
            df,
            'sum_skip',
            f"Total Skip per Hour per {groupByCondition}",
            'Total Skip',
            'Time',
            f"{prefix}_skip_per_hour_and_{groupByCondition}",
            [groupByCondition],
        ),
    ])


def createGraphByNamespace(config, result, prefix,  all_plot_args):
    createGraphBy(config, result, 'namespace', prefix,  all_plot_args)

def createGraphByDb(config, result, prefix,  all_plot_args):
    createGraphBy(config, result, 'db', prefix,  all_plot_args)

def createAndInsertGraphs(config, prefix, report, result):
    """
    Create graphs in parallel and insert them into the report PDF.
    Only add subtitle + move cursor down on the last graph.
    """
    if not getattr(config, "INSERT_GRAPH_SUMMARY_TO_REPORT", False):
        return

    if report:
        report.subChapter_title("Graphics")

        # Collect all plot arguments
    all_plot_args = []
    createGraphByDb(config, result, prefix, all_plot_args)
    createGraphByNamespace(config, result, prefix, all_plot_args)

    # Step 1: Generate all graphs concurrently
    plot_results = parallel_plot_tasks(all_plot_args)

    total_graphs = len(plot_results)

    # Step 2: Insert graphs into report
    for idx, (args, file_paths) in enumerate(plot_results):
        title = args[3] if len(args) > 3 else "Graph"

        # Handle missing or empty results
        if not file_paths:
            if report:
                report.chapter_body(f"No {title} to present as graph")
            continue

        if report:
            # Only move cursor down if this is the last graph
            move_down_flag = (idx == total_graphs - 1)

            img_path = file_paths[0]
            report.add_image(img_path, move_cursor_down=move_down_flag,aspect_ratio= 1)

            # Step 3: Cleanup images if configured
        if getattr(config, "DELETE_IMAGE_AFTER_USED", False):
            for file_path in file_paths:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"{file_path} has been deleted.")





                # Function to plot each metric
def plot_metric(process,metric_name, timestamps, values,prefix):
    plt.figure(figsize=(5, 5))
    plt.plot(timestamps, values, marker='o')
    plt.title(f"Metric: {metric_name}", color='black')
    plt.xlabel("Timestamp", color='black')
    plt.ylabel("Value", color='black')
    plt.xticks(rotation=45)
    plt.tight_layout()
    svg_file_path = f"{prefix}_{metric_name}.svg"
    svg_file_path = svg_file_path.replace(':', '_')
    plt.savefig(svg_file_path, format="svg")
    graph_logging.info(f"Metric graph saved to {svg_file_path}")
    return svg_file_path

from datetime import datetime

def plot_all_metricsForProcess(process, report, config, prefix):
    """
    Plot all defined metrics for the given process.
    Only add subtitle and move cursor down for the last graph in the sequence.
    """
    measurements_future = process.get("future", {}).get("measurement", None)
    if measurements_future is None:
        return

    measurements = measurements_future.result()
    #for measure in measurements.get('measurements',[]) :
    #   print(measure.get('name',""))
    if report:
        report.sub2Chapter_title("Metrics graph")

    # Get list of group names from config
    list_of_groups = config.get_template("sections.cluster.per_node.graph.metrics", [])
    group_defs = config.get_template("sections.cluster.per_node.graph.group_of_metrics", {})

    # Map metric name -> measurement dict for faster lookups
    measurements_map = {m["name"]: m for m in measurements.get("measurements", [])}

    # Determine the last group with data so we can mark it
    valid_groups = []
    for group_name in list_of_groups:
        group_info = group_defs.get(group_name, {})
        metric_list = group_info.get("list_of_metrics", [])

        any_valid_data = False
        for metric_name in metric_list:
            measurement = measurements_map.get(metric_name)
            if not measurement:
                continue
            data_points = measurement["dataPoints"]
            if any(dp['value'] is not None and dp['value'] != 0 for dp in data_points):
                any_valid_data = True
                break

        if any_valid_data:
            valid_groups.append(group_name)

    if not valid_groups:
        print("No metrics available for plotting.")
        return

    last_group = valid_groups[-1]  # The last group with actual data

    # Loop over each group again to actually plot
    for group_name in list_of_groups:
        group_info = group_defs.get(group_name, {})
        metric_list = group_info.get("list_of_metrics", [])

        group_timestamps_values = {}  # {metric_name: (timestamps, values)}
        any_valid_data = False

        for metric_name in metric_list:
            measurement = measurements_map.get(metric_name)
            if not measurement:
                print(f"Metric {metric_name} not found for group {group_name}")
                continue

            data_points = measurement["dataPoints"]

            # Filter non-zero and non-null
            if any(dp['value'] is not None and dp['value'] != 0 for dp in data_points):
                any_valid_data = True

                # Always store not-null data for plotting
            not_none_data = [(dp['timestamp'], dp['value']) for dp in data_points if dp['value'] is not None]
            if not_none_data:
                timestamps, values = zip(*not_none_data)
                timestamps = [datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") for ts in timestamps]
                group_timestamps_values[metric_name] = (timestamps, values)

        if any_valid_data:
            file = plot_metric_group(process, group_name, group_timestamps_values, prefix)
            if report:
                move_down_flag = (group_name == last_group)
                report.add_image(file, move_cursor_down=move_down_flag)
        else:
            print(f"Skipped group {group_name} as all metrics are None or zero.")


def plot_metric_group(process, group_name, metrics_data, prefix):
    """
    metrics_data: dict {metric_name: (timestamps, values)}
    Plot multiple metrics on the same figure and return file name.
    """
    plt.figure(figsize=(10, 10))
    for metric_name, (timestamps, values) in metrics_data.items():
        plt.plot(timestamps, values, label=metric_name)

    plt.title(f"Metrics Group: {group_name}", color='black')
    plt.xlabel("Time", color='black')
    plt.ylabel("Value", color='black')
    plt.legend()
    plt.grid(True)

    file_name = f"{prefix}_{group_name}.svg"
    plt.savefig(file_name)
    plt.close()
    return file_name

def plot_sku_monthly_costs(config, sku_data_json, title, output_file=None):
    """
    sku_data_json = dict returned from get_cluster_billing_sku_evolution()
    Produces a bar chart for each month with SKUs as bars, labeled with cost/%/evolution.
    """
    if config.GENERATE_PNG:
        if matplotlib.get_backend() != "cairo":
            matplotlib.use("cairo")
    elif matplotlib.get_backend() != "svg":
        matplotlib.use("svg")

        # Convert JSON structure to DataFrame
    records = []
    for month, month_info in sku_data_json.items():
        totalcost = month_info.get("totalcost", 0)
        evol_prev = month_info.get("evolution_previous_month_in_perc", None)
        evol_start = month_info.get("evolution_from_range_start_in_perc", None)
        for sku, sku_info in month_info.get("sku", {}).items():
            records.append({
                "month": month,
                "sku": sku,
                "cost": sku_info.get("cost", 0),
                "percent_monthly_cost": sku_info.get("percent_monthly_cost", 0),
                "evol_prev": sku_info.get("evolution_previous_month_in_perc", None),
                "evol_start": sku_info.get("evolution_from_range_start_in_perc", None),
                "month_evol_prev": evol_prev,
                "month_evol_start": evol_start
            })
    df = pd.DataFrame(records)

    if df.empty:
        return []

        # Keep top 15 SKUs per month by cost for readability
    top_skus = []
    for month in df['month'].unique():
        top = df[df['month'] == month].nlargest(15, 'cost')['sku']
        top_skus.extend(top)
    df = df[df['sku'].isin(top_skus)]

    months = sorted(df['month'].unique())
    num_months = len(months)
    fig, axes = plt.subplots(num_months, 1, figsize=(14, 6 * num_months))

    if num_months == 1:
        axes = [axes]  # make iterable

    for ax, month in zip(axes, months):
        month_df = df[df['month'] == month].sort_values(by='cost', ascending=False)
        bars = ax.bar(month_df['sku'], month_df['cost'], color='skyblue')

        ax.set_title(f"{title} - {month} (Total: {month_df['cost'].sum():,.2f})", fontsize=14)
        ax.set_ylabel("Cost")
        ax.set_xlabel("SKU")
        ax.tick_params(axis='x', rotation=90)

        # Annotate bars
        for bar, (_, row) in zip(bars, month_df.iterrows()):
            label_parts = [f"${row['cost']:,.0f}", f"{row['percent_monthly_cost']:.1f}%"]
            if row['evol_prev'] is not None:
                label_parts.append(f"{row['evol_prev']:+.1f}% vs prev")
            if row['evol_start'] is not None:
                label_parts.append(f"{row['evol_start']:+.1f}% vs start")
            label = "\n".join(label_parts)
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                label,
                ha='center', va='bottom', fontsize=8, rotation=90
            )

    plt.tight_layout()

    file_paths = []
    if output_file:
        output_file = output_file.replace(':', '_')
        if config.GENERATE_PNG:
            png_file_path = f"{output_file}.png"
            plt.savefig(png_file_path, bbox_inches='tight')
            file_paths.append(png_file_path)
        svg_file_path = f"{output_file}.svg"
        plt.savefig(svg_file_path, format="svg", bbox_inches='tight')
        file_paths.append(svg_file_path)

    plt.close(fig)
    return file_paths
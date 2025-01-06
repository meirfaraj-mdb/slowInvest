from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from matplotlib import pyplot as plt
import matplotlib

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
            print(f"Graph saved to {png_file_path}")
            file_paths.append(png_file_path)

        svg_file_path = f"{output_file}.svg"
        plt.savefig(svg_file_path, format="svg")
        print(f"Graph saved to {svg_file_path}")
        file_paths.append(svg_file_path)

    plt.close(fig)
    return file_paths


def parallel_plot_tasks(plot_args_list):
    """Run multiple plot tasks in parallel and collect their results."""
    results = []
    with ProcessPoolExecutor() as executor:
        future_to_args = {executor.submit(plot_stats, *args): args for args in plot_args_list}
        for future in as_completed(future_to_args):
            try:
                result = future.result()
                results.append((future_to_args[future], result))
            except Exception as e:
                print(f"An error occurred while processing: {e}")
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
    if not config.INSERT_GRAPH_SUMMARY_TO_REPORT:
        return

    report.subChapter_title("Graphics")

    # Collect all plot arguments
    all_plot_args = []
    createGraphByDb(config, result, prefix,  all_plot_args)
    createGraphByNamespace(config, result, prefix,  all_plot_args)

    # Step 1: Generate all graphs concurrently
    plot_results = parallel_plot_tasks(all_plot_args)

    # Step 2: Integrate all generated graphs into the report in order
    for args,file_paths in plot_results:
        if file_paths and len(file_paths)==0:
           if report:
              report.chapter_body(f"No {args[3]} to present as graph")

        if report:
            report.add_page()
            report.sub2Chapter_title(args[3])
            report.add_image(file_paths[0])

        for file_path in file_paths:
            # Optionally delete the images after usage
            if config.DELETE_IMAGE_AFTER_USED and os.path.exists(file_path):
                os.remove(file_path)
                print(f"{file_path} has been deleted.")

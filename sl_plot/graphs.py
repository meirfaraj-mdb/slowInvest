from matplotlib import pyplot as plt


def plot_stats(config, df, value_col, title, ylabel, xlabel='Time', output_file=None, columns=['namespace'], report=None):
    if df.empty:
        if report:
            report.chapter_body(f"No {title} to present as graph")
        return

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

    if output_file:
        # Replace colons with underscores in the file name.
        output_file = output_file.replace(':', '_')

        if config.GENERATE_PNG:
            plt.savefig(output_file, bbox_inches='tight')
            print(f"Graph saved to {output_file}")

        file_path = f"{output_file}.svg"
        plt.savefig(file_path, format="svg")
        print(f"Graph saved to {file_path}")

        if report:
            report.add_image(file_path)
            if config.DELETE_IMAGE_AFTER_USED and os.path.exists(file_path):
                os.remove(file_path)
                print(f"{file_path} has been deleted.")
    else:
        plt.show()


def createGraphByNamespace(config, result, prefix, report):
    groupByCondition = 'namespace'
    createGraphBy(config, result, groupByCondition, prefix, report)


def createGraphByDb(config, result, prefix, report):
    groupByCondition = 'db'
    createGraphBy(config, result, groupByCondition, prefix, report)


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


def createGraphBy(config, result, groupByCondition, prefix, report):
    if config.INSERT_GRAPH_SUMMARY_TO_REPORT:
      if result["groupByCommandShape"]["hours"] is None or len(result["groupByCommandShape"]["hours"])==0:
          return
      df = aggregateForGraph(result["groupByCommandShape"]["hours"],['hour', groupByCondition])
      all_group = df[groupByCondition].unique()
      report.chapter_body(f"{groupByCondition} List : {all_group}")
      df_plan_summary = aggregateForGraph(result["groupByCommandShape"]["hours"],['hour', groupByCondition, 'plan_summary'])

      # Plot number of slow queries per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"Number of Slow Queries per Hour per {groupByCondition}")
      file_name = f"{prefix}_slow_queries_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df, 'slow_query', f"Number of Slow Queries per Hour per {groupByCondition}",
                   'Number of Slow Queries',
                   output_file=file_name, report=report,columns=[groupByCondition])

      # Plot total duration of slow queries per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"Total Duration of Slow Queries per Hour per {groupByCondition}")
      file_name = f"{prefix}_total_duration_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df, 'total_duration', f"Total Duration of Slow Queries per Hour per {groupByCondition}",
                   'Total Duration (ms)',
                   output_file=file_name, report=report,columns=[groupByCondition])

      #    tmp = df[df['plan_summary'].str.contains('COLLSCAN')]
      report.sub2Chapter_title(f"COLLSCAN Count per Hour per {groupByCondition}")
      report.add_page()
      file_name = f"{prefix}_COLLSCAN_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df_plan_summary[df_plan_summary['plan_summary'].str.contains('COLLSCAN')],
                   'slow_query', f"COLLSCAN Count per Hour per {groupByCondition}", 'COLLSCAN Count',
                   output_file=file_name, columns=[groupByCondition, 'plan_summary'], report=report)

      # Plot hasSortStage count per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"Has Sort Stage Count per Hour per {groupByCondition}")
      file_name = f"{prefix}_has_sort_stage_per_hour.png"
      plot_stats(config, df, "has_sort_stage", f"Has Sort Stage Count per Hour per {groupByCondition}",
                   "Has Sort Stage Count", output_file=file_name, report=report,columns=[groupByCondition])

      # Plot writeConflicts count per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"write conflicts Count per Hour per {groupByCondition}")
      file_name = f"{prefix}_writeConflicts_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df, 'writeConflicts', f"write conflicts Count per Hour per {groupByCondition}",
                   'write conflicts Count',
                   output_file=file_name, report=report,columns=[groupByCondition])

      # Plot query targeting per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"Query Targeting per Hour per {groupByCondition}")
      file_name = f"{prefix}_query_targeting_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df, 'query_targeting', f"Query Targeting per Hour per {groupByCondition}", 'Query Targeting',
                   output_file=file_name, report=report,columns=[groupByCondition])

      # Plot query targeting per hour per groupByCondition
      report.add_page()
      report.sub2Chapter_title(f"Skip per Hour per {groupByCondition}")
      file_name = f"{prefix}_skip_per_hour_and_{groupByCondition}.png"
      plot_stats(config, df, 'sum_skip', f"total skip per Hour per {groupByCondition}", 'total skip',
                   output_file=file_name, report=report,columns=[groupByCondition])


#  pip install -r requirements.txt

import matplotlib.pyplot as plt
from tabulate import tabulate
from AtlasApi import AtlasApi
from report import Report
from slowQuery import *
from config import Config
import sys
import os
import concurrent.futures


#---------------------------------------------------------------------------
# Plot Util :
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





#-----------------------------------------------------------------------------------------
# markdown utility :

def save_markdown(df,fileName,comment):
    if not config.GENERATE_MD:
        return
    # Convert the DataFrame to a markdown table
    markdown_table = tabulate(df, headers='keys', tablefmt='pipe')

    # Save the markdown table to a file
    with open(fileName, 'w') as f:
        f.write(markdown_table)

    print("\nStatistics by Command "+comment+" Shape (sorted by average duration) have been saved to '"+fileName+"'")



#----------------------------------------------------------------------------------------
# display slow query
def process_row(index, row,report,columns):
    # Define a function to apply to each row
    report.add_page()
    report.sub4Chapter_title(
        f"{convertToHumanReadable('cmdType',row['cmdType'])}"
        f"({convertToHumanReadable('namespace',row['namespace'])}) :"
        f" Time={convertToHumanReadable('durationMillis',row['durationMillis_total'],True)}, count={row['slow_query_count']}"+
        (f" DISK={convertToHumanReadable('bytesTotalDiskWR',row['storage_data_bytesTotalDiskWR_total'], True)}" if row['storage_data_bytesTotalDiskWR_total']>0 else "")+
        (f" RES={convertToHumanReadable('bytesReslen',row['bytesReslen_total'], True)}" if row['bytesReslen_total']>0 else ""))


    report.add_json(row['command_shape'])
    report.add_page()
    report.table(row.drop(columns=['command_shape']), [col for col in columns if col != 'command_shape'])



def display_queries(reportTitle,report, df):
    if df is None or df.empty :
        return
    report.add_page()
    report.sub2Chapter_title(reportTitle)
    df = df.copy()
    # Ensure the column used for grouping contains hashable types
    if 'app_name' in df.columns:
        df['app_name'] = df['app_name'].apply(
            lambda x: tuple(x) if isinstance(x, list) else x
        )
    # Group the DataFrame by appName
    grouped_by_app = df.groupby('app_name')

    # Iterate over each appName group
    for app_name, app_group in grouped_by_app:
        # Call report.sub3Chapter_title with the appName
        report.sub3Chapter_title(f"appName: {app_name}")

        # Sort the grouped DataFrame by slow_query_count and average_duration in descending order
        sorted_grouped_df = app_group.sort_values(by=['durationMillis_total','slow_query_count'], ascending=[False, False])

        # Iterate over each row in the sorted grouped DataFrame
        for index, row in sorted_grouped_df.iterrows():
            process_row(index, row, report, sorted_grouped_df.columns)



#----------------------------------------------------------------------------------------
# report

def addToReport(result,prefix,report,config):
    report.chapter_title(f"Slow Query Report Summary : {prefix}")
    report.subChapter_title("General")
    report.chapter_body(f"Instance : {prefix}")
    if result.get("countOfSlow",0) == 0:
        report.chapter_body("No slow query!")
        report.add_page()
        return

    # Aggregate the data to ensure unique hour-namespace combinations
    if config.INSERT_GRAPH_SUMMARY_TO_REPORT:
        report.subChapter_title("Graphics")
        createGraphByDb(config, result, prefix, report)
        createGraphByNamespace(config, result, prefix, report)

    # Group by command shape and calculate statistics
    command_shape_stats = result["groupByCommandShape"].get("global",{})
    if len(command_shape_stats)>0 and command_shape_stats.shape[0]>0:
        addCommandShapAnalysis(command_shape_stats, config, report)

    ################## change stream
    command_shape_cs_stats = result["groupByCommandShapeChangeStream"].get("global",{})
    if (command_shape_cs_stats is not None) and len(command_shape_cs_stats)>0 and command_shape_cs_stats.shape[0]>0:
        save_markdown(command_shape_cs_stats, 'command_shape_cs_stats.md', "changestream")
        display_queries("List of changestream",report,command_shape_cs_stats)


def addCommandShapAnalysis(command_shape_stats, config, report):
    if config.MINIMUM_DURATION_FOR_QUERYSHAPE > 0:
        command_shape_stats = command_shape_stats[
            command_shape_stats['durationMillis_total'] >= (1000 * config.MINIMUM_DURATION_FOR_QUERYSHAPE)]
    # Sort by average duration
    filtered_df = command_shape_stats[command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # Create DataFrame excluding filtered rows
    remaining_df = command_shape_stats[~command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # bad query targeting
    # Further split remaining_df
    sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == True]
    no_sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == False]
    with_conflict = no_sort_stage_df[no_sort_stage_df['writeConflicts_total'] > 0]
    without_conflict = no_sort_stage_df[no_sort_stage_df['writeConflicts_total'] == 0]
    has_skip = without_conflict[without_conflict['skip_total'] > 0]
    without_skip = without_conflict[without_conflict['skip_total'] == 0]
    # $in
    has_badIn = without_skip[without_skip['max_count_in_max'] > 200]
    without_badIn = without_skip[without_skip['max_count_in_max'] <= 200]
    # regex
    # array filter
    ##without_badIn
    report.subChapter_title("List of slow query shape")
    save_markdown(filtered_df, 'command_shape_collscan_stats.md', "collscan")
    display_queries("List of Collscan query shape", report, filtered_df)
    save_markdown(sort_stage_df, 'command_shape_remaining_hasSort_stats.md', "remainingHasSort")
    display_queries("List of remain hasSortStage query shape", report, sort_stage_df)
    save_markdown(with_conflict, 'withConflict_stats.md', "withConflict")
    display_queries("List of other query withConflict", report, with_conflict)
    ###################SKIP
    save_markdown(has_skip, 'has_skip_stats.md', "has_skip")
    display_queries("List of other query with skip", report, has_skip)
    ###################bad $in
    save_markdown(has_skip, 'has_badIn.md', "has_badIn")
    display_queries("List of query with bad $in", report, has_badIn)
    ###################$Regex
    ###################disk
    save_markdown(without_badIn, 'command_shape_others_stats.md', "others")
    display_queries("List of other query shape", report, without_badIn)


def createGraphByNamespace(config, result, prefix, report):
    groupByCondition = 'namespace'
    createGraphBy(config, result, groupByCondition, prefix, report)

def createGraphByDb(config, result, prefix, report):
    groupByCondition = 'db'
    createGraphBy(config, result, groupByCondition, prefix, report)

# TODO: has sort stage need fix
def aggregateForGraph(df,groupByConditions):
    for cond in groupByConditions:
        if cond != 'hour':
            df[cond] = df[cond].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    return df.groupby(groupByConditions).agg(
        slow_query_count=('slow_query_count', 'sum'),
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
      plot_stats(config, df, 'slow_query_count', f"Number of Slow Queries per Hour per {groupByCondition}",
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
                   'slow_query_count', f"COLLSCAN Count per Hour per {groupByCondition}", 'COLLSCAN Count',
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


def display_cluster(config,report,cluster):
    report.display_cluster_table(cluster)

def atlas_retrieval_mode(config,report):
    atlasApi = AtlasApi(config)
    if config.ATLAS_RETRIEVAL_SCOPE == "project":
        print(f"Get project {config.GROUP_ID} composition....")
        start_time_comp = time.time()
        compositions = atlasApi.get_clusters_composition(config.GROUP_ID,full=config.GENERATE_INFRA_REPORT)
        end_time_comp = time.time()
        print(f"Received project {config.GROUP_ID} composition in {convertToHumanReadable("Millis",(end_time_comp-start_time_comp)*1000,True)}")
        for cluster in compositions :
            generate_cluster_report(atlasApi, cluster, config, report)


    elif config.ATLAS_RETRIEVAL_SCOPE == "clusters":
        for cluster_name in config.CLUSTERS_NAME:
            print(f"Get cluster {cluster_name} composition....")
            start_time_comp = time.time()
            compositions = atlasApi.get_clusters_composition(config.GROUP_ID,cluster_name,full=config.GENERATE_INFRA_REPORT)
            end_time_comp = time.time()
            print(f"Received cluster {cluster_name} composition in {convertToHumanReadable("Millis",(end_time_comp-start_time_comp)*1000,True)}")
            for cluster in compositions :
                generate_cluster_report(atlasApi, cluster, config, report)

    else: #processId
        for process in config.PROCESSES_ID:
            path=f"{config.OUTPUT_FILE_PATH}/process/{process}/"
            addToReport(atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,process,path,
                                                                       config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK),
                        process,
                        report,
                        config)


def generate_cluster_report(atlasApi, cluster, config, report):
    if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        report = Report(config)
        report.add_page()
    processesShard = cluster.get("processes", {})
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(processesShard)*4+3,20))
    results={}
    for shard_num, processes in processesShard.items():
        if shard_num == "config":
            path=f"{config.OUTPUT_FILE_PATH}/{cluster.get("name")}/{processes.get("typeName")}/{processes.get("hostname")}/"
            results[processes.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, processes.get("id", ""),path,config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)
            continue
        primary = processes.get("primary", {})
        #atlasApi.get_database_composition_for_process(cluster, primary)
        path=f"{config.OUTPUT_FILE_PATH}/{cluster.get("name")}/{primary.get("typeName")}/{primary.get("hostname")}/"
        results[primary.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, primary.get("id", ""),
                                                           path,
                                                           config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)
        #atlasApi.get_database_composition_sizing_for_process(cluster, primary)

        others = processes.get("others", {})
        for proc in others:
            path=f"{config.OUTPUT_FILE_PATH}/{cluster.get("name")}/{proc.get("typeName")}/{proc.get("hostname")}/"
            results[proc.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, proc.get("id", ""),
                                                               path,
                                                               config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)

    pool.shutdown(wait=True)
    display_cluster(config, report, cluster)

    for shard_num, processes in processesShard.items():
        if shard_num == "config":
            addToReport(
                results[processes.get("id","")].result(),
                f"{config.OUTPUT_FILE_PATH}/{shard_num}_{processes.get("id", "")}_{processes.get("typeName", "")}",
                report,
                config)
            continue
        primary = processes.get("primary", {})
        #atlasApi.get_database_composition_for_process(cluster, primary)
        addToReport(
            results[primary.get("id", "")].result(),
            f"{config.OUTPUT_FILE_PATH}/{shard_num}_{primary.get("id", "")}_{primary.get("typeName", "")}",
            report,
            config)
        atlasApi.get_database_composition_sizing_for_process(cluster, primary)

        others = processes.get("others", {})
        for proc in others:
            addToReport(
                results[proc.get("id", "")].result(),
                f"{config.OUTPUT_FILE_PATH}/{shard_num}_{proc.get("id", "")}_{proc.get("typeName", "")}",
                report,
                config)
    if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        report.write(f"{config.REPORT_FILE_PATH}/slow_report{cluster.get("name")}")


def file_retrieval_mode(config,report):
    for file in config.LOGS_FILENAME:
        if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
            report = Report(config)
            report.add_page()
        addToReport(extract_slow_queries(f"{config.INPUT_PATH}/{file}", f"{config.OUTPUT_FILE_PATH}/slow_queries_{file}",
                                         config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK),
                    f"{config.OUTPUT_FILE_PATH}/{file}",
                    report,
                    config)
        if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
            report.write(f"{config.REPORT_FILE_PATH}/slow_report{file}")

#----------------------------------------------------------------------------------------
#  Main :
if __name__ == "__main__":
    if sys.version_info[0:2] != (3, 12):
        raise Exception('Requires python 3.12')
    start_time_all=time.time()
    first_option = sys.argv[1] if len(sys.argv) > 1 else None
    # Use the first_option in your Config class or elsewhere
    config = Config(first_option)
    report = Report(config)
    report.add_page()

    if config.RETRIEVAL_MODE == "Atlas":
        atlas_retrieval_mode(config,report)
    elif config.RETRIEVAL_MODE == "files":
        file_retrieval_mode(config,report)
    elif config.RETRIEVAL_MODE == "OpsManager":
        print(f"Retrieval Mode: {config.RETRIEVAL_MODE} not yet implemented use files mode instead.")
    else:
        raise ValueError(f"Unsupported RETRIEVAL_MODE: {config.RETRIEVAL_MODE}")

    if not config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        report.write(f"{config.REPORT_FILE_PATH}/slow_report")
    end_time_all=time.time()
    print(f"work end in {convertToHumanReadable("Millis",(end_time_all-start_time_all)*1000)}")

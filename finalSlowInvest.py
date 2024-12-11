# python.exe -m pip install --upgrade pip
# pip install requests
# pip install pandas
# pip install seaborn
# pip install matplotlib
# pip install tabulate
# pip install PdfReader
#pip uninstall fpdf2
#pip install git+https://github.com/andersonhc/fpdf2.git@page-number

from collections import defaultdict
import seaborn as sns
import matplotlib.pyplot as plt
from tabulate import tabulate

from AtlasApi import AtlasApi
from report import Report
from utils import *
from slowQuery import *
from config import Config
import sys
import os



#---------------------------------------------------------------------------
# Plot Util :
def plot_stats(config,df, value_col, title, ylabel, xlabel='Time', output_file=None, columns = ['namespace'],report=None):
    if df.empty:
        if report:
            report.chapter_body(f"No {title} to present as graph")
        return
    # Create a pivot table with hour and namespace
    pivot_df = df.pivot_table(index='hour', columns= columns, values=value_col, aggfunc='sum', fill_value=0)

    # Plot the data
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='line', ax=ax)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title='Namespace', loc='upper left', bbox_to_anchor=(1, 1))
    plt.xticks(rotation=45)
    plt.tight_layout()

    if output_file:
        if config.GENERATE_PNG:
            plt.savefig(output_file, bbox_inches='tight')
            print(f"Graph saved to {output_file}")
        file_path=f"{output_file}.svg"
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
        (f" totalW/R={convertToHumanReadable('bytesTotalDiskWR',row['storage_data_bytesTotalDiskWR_total'], True)}" if row['storage_data_bytesTotalDiskWR_total']>0 else ""))


    report.add_json(row['command_shape'])
    report.add_page()
    report.table(row.drop(columns=['command_shape']), [col for col in columns if col != 'command_shape'])



def display_queries(reportTitle,report, df):
    if df.empty :
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

def distinct_values(series):
    # Drop NaN values and get unique values as a list
    unique_values = series.dropna().unique().tolist()
    # Convert the list of unique values to a string separated by commas
    return ', '.join(map(str, unique_values))
    # Other utilities
#def distinct_values(series):
#    return ', '.join(sorted(set(series)))

def minMaxAvgTtl(column_name):
    """Generate a dictionary of aggregation operations for a given column."""
    return {
        f'{column_name}_min': (column_name, 'min'),
        f'{column_name}_max': (column_name, 'max'),
        f'{column_name}_avg': (column_name, 'mean'),
        f'{column_name}_total': (column_name, 'sum')
    }
def groupbyCommandShape(df):
    # Create a dictionary to hold all aggregation operations
    agg_operations = {
        'slow_query_count': ('slow_query_count', 'sum'),
        'has_sort_stage': ('has_sort_stage', lambda x: x.mode().iloc[0] if not x.mode().empty else False),
        'usedDisk': ('usedDisk', distinct_values),
        'fromMultiPlanner': ('fromMultiPlanner', distinct_values),
        'replanned':('replanned', distinct_values),
        'replanReason':('replanReason', distinct_values),
        'plan_summary': ('plan_summary', distinct_values),
        'app_name': ('appName', distinct_values),
        'namespace': ('namespace', distinct_values),
        'cmdType':('cmdType', distinct_values),
        'count_of_in':('count_of_in', distinct_values),
        'getMore':('getMore','sum')
    }

    # Add min, max, avg, total for each specified column
    for column in ['writeConflicts','durationMillis','planningTimeMicros', 'keys_examined', 'docs_examined', 'nreturned', 'query_targeting',
                   'nBatches', 'numYields','skip','limit', 'waitForWriteConcernDurationMillis','totalOplogSlotDurationMicros',
                   'ninserted','keysInserted','bytesReslen','flowControl_acquireCount','flowControl_timeAcquiringMicros',
                   'storage_data_bytesRead','storage_data_timeReadingMicros','ninserted', 'nMatched', 'nModified','nUpserted','ndeleted',
                   'keysInserted','keysDeleted','bytesReslen','flowControl_acquireCount','flowControl_timeAcquiringMicros',
                   'storage_data_bytesRead','storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
                   'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros','max_count_in','sum_of_counts_in']:
        agg_operations.update(minMaxAvgTtl(column))
    return df.groupby('command_shape').agg(**agg_operations).reset_index()

def addToReport(df,prefix,report):
    report.chapter_title(f"Slow Query Report Summary : {prefix}")
    report.subChapter_title("General")
    report.chapter_body(f"Instance : {prefix}")
    df = df[~df['namespace'].str.startswith(('admin', 'local', 'config'))]
    df_orig = df
    # Aggregate the data to ensure unique hour-namespace combinations
    df = df.groupby(['hour', 'namespace']).agg(
        slow_query_count=('slow_query_count', 'sum'),
        total_duration=('durationMillis', 'sum'),
        writeConflicts=('writeConflicts', 'sum'),
        has_sort_stage=('has_sort_stage', 'sum'),
        sum_skip=('skip', 'sum'),
        query_targeting=('query_targeting', 'max')).reset_index()
    all_namespaces = df['namespace'].unique()
    report.chapter_body(f"Namespace List : {all_namespaces}")
    if df.empty :
        report.chapter_body("No slow query!")
        report.add_page()
        return

    report.subChapter_title("Graphics")
    df_changestream = df_orig[df_orig['changestream'] == True]
    df_withoutChangestream = df_orig[df_orig['changestream'] == False]
    df_plan_summary = df_withoutChangestream.groupby(['hour', 'namespace', 'plan_summary']).agg(
    slow_query_count=('slow_query_count', 'sum'),
    writeConflicts=('writeConflicts', 'sum'),
    total_duration=('durationMillis', 'sum'),
    has_sort_stage=('has_sort_stage', 'sum'),
    sum_skip=('skip', 'sum'),
    query_targeting=('query_targeting', 'max')).reset_index()

    # Plot number of slow queries per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Number of Slow Queries per Hour per Namespace")
    file_name = f"{prefix}_slow_queries_per_hour.png"
    plot_stats(config,df, 'slow_query_count', 'Number of Slow Queries per Hour per Namespace', 'Number of Slow Queries',
               output_file=file_name,report=report)

    # Plot total duration of slow queries per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Total Duration of Slow Queries per Hour per Namespace")
    file_name = f"{prefix}_total_duration_per_hour.png"
    plot_stats(config,df, 'total_duration', 'Total Duration of Slow Queries per Hour per Namespace', 'Total Duration (ms)',
               output_file=file_name,report=report)

#    tmp = df[df['plan_summary'].str.contains('COLLSCAN')]
    report.add_page()
    report.sub2Chapter_title("COLLSCAN Count per Hour per Namespace")
    file_name = f"{prefix}_COLLSCAN_per_hour.png"
    plot_stats(config,df_plan_summary[df_plan_summary['plan_summary'].str.contains('COLLSCAN')], 'slow_query_count', 'COLLSCAN Count per Hour per Namespace', 'COLLSCAN Count',
           output_file=file_name,columns = ['namespace','plan_summary'],report=report)

    # Plot hasSortStage count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Has Sort Stage Count per Hour per Namespace")
    file_name = f"{prefix}_has_sort_stage_per_hour.png"
    plot_stats(config,df, 'has_sort_stage', 'Has Sort Stage Count per Hour per Namespace', 'Has Sort Stage Count',
               output_file=file_name,report=report)

    # Plot writeConflicts count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("write conflicts Count per Hour per Namespace")
    file_name = f"{prefix}_writeConflicts_per_hour.png"
    plot_stats(config,df, 'writeConflicts', 'write conflicts Count per Hour per Namespace', 'write conflicts Count',
               output_file=file_name,report=report)

    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Query Targeting per Hour per Namespace")
    file_name = f"{prefix}_query_targeting_per_hour.png"
    plot_stats(config,df, 'query_targeting', 'Query Targeting per Hour per Namespace', 'Query Targeting',
               output_file=file_name,report=report)


    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Skip per Hour per Namespace")
    file_name = f"{prefix}_skip_per_hour.png"
    plot_stats(config,df, 'sum_skip', 'total skip per Hour per Namespace', 'total skip',
               output_file=file_name,report=report)


    # Group by command shape and calculate statistics
    command_shape_stats = groupbyCommandShape(df_withoutChangestream)
    # Sort by average duration
    filtered_df = command_shape_stats[command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # Create DataFrame excluding filtered rows
    remaining_df = command_shape_stats[~command_shape_stats['plan_summary'].str.contains("COLLSCAN", na=False)]
    # bad query targeting

    # Further split remaining_df
    sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == True]
    no_sort_stage_df = remaining_df[remaining_df['has_sort_stage'] == False]

    with_conflict = no_sort_stage_df[no_sort_stage_df['writeConflicts_total'] >0]
    without_conflict = no_sort_stage_df[no_sort_stage_df['writeConflicts_total'] == 0]

    has_skip = without_conflict[without_conflict['skip_total']>0]
    without_skip = without_conflict[without_conflict['skip_total']==0]

    # $in
    has_badIn = without_skip[without_skip['max_count_in_max']>200]
    without_badIn = without_skip[without_skip['max_count_in_max']<=200]

    # regex
    # array filter

    ##without_badIn


    report.subChapter_title("List of slow query shape")
    save_markdown(filtered_df, 'command_shape_collscan_stats.md', "collscan")
    display_queries("List of Collscan query shape",report,filtered_df)

    save_markdown(sort_stage_df, 'command_shape_remaining_hasSort_stats.md', "remainingHasSort")
    display_queries("List of remain hasSortStage query shape",report,sort_stage_df)

    save_markdown(with_conflict, 'withConflict_stats.md', "withConflict")
    display_queries("List of other query withConflict",report,with_conflict)

###################SKIP
    save_markdown(has_skip, 'has_skip_stats.md', "has_skip")
    display_queries("List of other query with skip",report,has_skip)

###################bad $in
    save_markdown(has_skip, 'has_badIn.md', "has_badIn")
    display_queries("List of query with bad $in",report,has_badIn)

###################$Regex
###################disk
    save_markdown(without_badIn, 'command_shape_others_stats.md', "others")
    display_queries("List of other query shape",report,without_badIn)

################## change stream
    command_shape_cs_stats = groupbyCommandShape(df_changestream)
    save_markdown(command_shape_cs_stats, 'command_shape_cs_stats.md', "changestream")
    display_queries("List of changestream",report,command_shape_cs_stats)

def display_cluster(config,report,cluster):
    report.display_cluster_table(cluster)

def atlas_retrieval_mode(config,report):
    atlasApi = AtlasApi(config)
    if config.ATLAS_RETRIEVAL_SCOPE == "project":
        compositions = atlasApi.get_clusters_composition(config.GROUP_ID)
        for cluster in compositions :
            display_cluster(config,report,cluster)
            processesShard=cluster.get("processes",{})
            for shard_num,processes in processesShard.items():
                if shard_num == "config":
                    addToReport(
                        atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,processes.get("id",""),config.OUTPUT_FILE_PATH),
                        f"{shard_num}_{processes.get("id","")}_{processes.get("typeName","")}",
                        report)
                    continue
                primary=processes.get("primary",{})
                addToReport(
                    atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,primary.get("id",""),config.OUTPUT_FILE_PATH),
                    f"{shard_num}_{primary.get("id","")}_{primary.get("typeName","")}",
                    report)
                others=processes.get("others",{})
                for proc in others:
                    addToReport(
                      atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,proc.get("id",""),config.OUTPUT_FILE_PATH),
                      f"{shard_num}_{proc.get("id","")}_{proc.get("typeName","")}",
                      report)

    elif config.ATLAS_RETRIEVAL_SCOPE == "clusters":
        for cluster_name in config.CLUSTERS_NAME:
            compositions = atlasApi.get_clusters_composition(config.GROUP_ID,cluster_name)
            for cluster in compositions :
               display_cluster(config,report,cluster)

    else: #processId
        for process in config.PROCESSES_ID:
            addToReport(atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,process,config.OUTPUT_FILE_PATH),process,report)

#----------------------------------------------------------------------------------------
#  Main :
if __name__ == "__main__":
    first_option = sys.argv[1] if len(sys.argv) > 1 else None
    # Use the first_option in your Config class or elsewhere
    config = Config(first_option)
    report = Report(config)
    report.add_page()

    if config.RETRIEVAL_MODE == "Atlas":
        atlas_retrieval_mode(config,report)
    elif config.RETRIEVAL_MODE == "files":
        for file in config.LOGS_FILENAME:
            addToReport(extract_slow_queries(f"{config.INPUT_PATH}/{file}", config.OUTPUT_FILE_PATH),
                        f"{config.INPUT_PATH}/{file}",report)
    elif config.RETRIEVAL_MODE == "OpsManager":
        print(f"Retrieval Mode: {config.RETRIEVAL_MODE} not yet implemented use files mode instead.")
    else:
        raise ValueError(f"Unsupported RETRIEVAL_MODE: {config.RETRIEVAL_MODE}")
    report.write(f"{config.REPORT_FILE_PATH}/slow_report")



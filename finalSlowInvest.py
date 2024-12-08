# python.exe -m pip install --upgrade pip
# pip install requests
# pip install pandas
# pip install seaborn
# pip install matplotlib
# pip install tabulate
# pip install PdfReader
#pip uninstall fpdf2
#pip install git+https://github.com/andersonhc/fpdf2.git@page-number

import requests
from requests.auth import HTTPDigestAuth
import json
from collections import defaultdict
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from tabulate import tabulate
from report import Report
from utils import *
from config import Config
import sys

DF_COL = ['hour', 'namespace', 'slow_query_count', 'durationMillis','planningTimeMicros', 'has_sort_stage', 'query_targeting',
             'plan_summary', 'command_shape', 'writeConflicts', 'skip', 'limit', 'appName', 'changestream', 'usedDisk',
              'fromMultiPlanner','replanned','replanReason',
             'keys_examined', 'docs_examined', 'nreturned', 'cursorid', 'nBatches', 'numYields',
             'totalOplogSlotDurationMicros', 'waitForWriteConcernDurationMillis', 'ninserted', 'nMatched', 'nModified',
             'nUpserted', 'ndeleted', 'keysInserted','keysDeleted', 'bytesReslen', 'flowControl_acquireCount', 'flowControl_timeAcquiringMicros',
             'storage_data_bytesRead', 'storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
              'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros',
             'cmdType','count_of_in','max_count_in','sum_of_counts_in','getMore']


#------------------------------------------------------------------------------------
# Function for extracting from Api
def atlas_request(op, fpath, fdate, arg):

    apiBaseURL = '/api/atlas/v2'
    url = f"https://cloud.mongodb.com{apiBaseURL}{fpath}"

    headers = {
        'Accept': f"application/vnd.atlas.{fdate}+json",
        'Content-Type': f"application/vnd.atlas.{fdate}+json"
    }

    response = requests.get(
        url,
        params=arg,
        auth=HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY),
        headers=headers
    )
    #print(f"{op} response: {response.text}")
    return json.loads(response.text)



# retrieve slow queries
def retrieveLast24HSlowQueriesFromCluster(groupId,processId, output_file_path):
    path=f"/groups/{groupId}/processes/{processId}/performanceAdvisor/slowQueryLogs"
    resp=atlas_request('SlowQueries', path, '2023-01-01', {})
    slow_queries = []
    data = []
    for entry in resp['slowQueries']:
        try:
            line = entry['line']
            log_entry = json.loads(line)
            if log_entry.get("msg") == "Slow query":
                extractSlowQueryInfos(data, line, log_entry, slow_queries)
        except json.JSONDecodeError:
            # Skip lines that are not valid JSON
            continue
    print(f"Extracted slow queries have been saved to {output_file_path}")
    return pd.DataFrame(data, columns=DF_COL)

#------------------------------------------------------------------------------------
# Function for extracting from File :
def extract_slow_queries(log_file_path, output_file_path):
    slow_queries = []
    data = []

    with open(log_file_path, 'r') as log_file:
        for line in log_file:
            try:
                log_entry = json.loads(line)
                if log_entry.get("msg") == "Slow query":
                    extractSlowQueryInfos(data, line, log_entry, slow_queries)
            except json.JSONDecodeError:
                # Skip lines that are not valid JSON
                continue

    with open(output_file_path, 'w') as output_file:
        for query in slow_queries:
            output_file.write(query)

    print(f"Extracted slow queries have been saved to {output_file_path}")
    return pd.DataFrame(data, columns=DF_COL)

def check_change_stream(document):
    changestream = False
    def search_for_change_stream(subdoc):
        if isinstance(subdoc, dict):
            for key, value in subdoc.items():
                if key == "$changeStream":
                    return True
                if isinstance(value, dict) or isinstance(value, list):
                    if search_for_change_stream(value):
                        return True
        elif isinstance(subdoc, list):
            for item in subdoc:
                if search_for_change_stream(item):
                    return True
        return False
    if search_for_change_stream(document):
        changestream = True
    return changestream

def extractSlowQueryInfos(data, line, log_entry, slow_queries):
    slow_queries.append(line)
    # Extract relevant fields
    getMore=0
    # Access the 'attr' dictionary
    attr = log_entry.get('attr', {})
    # Get the 'type' value and convert it to lowercase
    type_value = attr.get('type', 'unknown').lower()

    # Access the 'command' dictionary
    command = attr.get('command', {})

    # Get the first attribute name in the 'command' dictionary
    first_attribute_name = next(iter(command), 'unknown')

    # Return the formatted string
    cmdType = f"{type_value}.{first_attribute_name}"

    namespace = attr.get("ns", "unknown")
    totalOplogSlotDurationMicros = attr.get("totalOplogSlotDurationMicros", 0)
    waitForWriteConcernDurationMillis = attr.get("waitForWriteConcernDurationMillis", 0)

    duration = attr.get("durationMillis", 0)
    planningTimeMicros = attr.get("planningTimeMicros", 0)
    has_sort_stage = 1 if attr.get("hasSortStage", False) else 0
    keys_examined = attr.get("keysExamined", 0)
    docs_examined = attr.get("docsExamined", 0)
    nreturned = attr.get("nreturned", 0)  # Default to 0 if not defined
    appName = attr.get("appName", "n")
    query_targeting = max(keys_examined, docs_examined) / nreturned if nreturned > 0 else 0
    #if(query_targeting>1):
    #    print(f" query_targeting={query_targeting}, nreturned={nreturned} , keys_examined={keys_examined}, docs_examined={docs_examined}")
    plan_summary = attr.get("planSummary", "n")
    cursorid = attr.get("cursorid", 0)
    nBatches = attr.get("nBatches", 0)
    numYields = attr.get("numYields", 0)

    ninserted = attr.get("ninserted", 0)
    keysInserted = attr.get("keysInserted", 0)
    keysDeleted = attr.get("keysDeleted", 0)
    nMatched = attr.get("nMatched", 0)
    nModified = attr.get("nModified", 0)
    nUpserted = attr.get("nUpserted", 0)
    ndeleted = attr.get("ndeleted", 0)
    reslen = attr.get("reslen", 0)
    usedDisk = attr.get("usedDisk", 0)

    fromMultiPlanner = attr.get("fromMultiPlanner", 0)
    replanned = attr.get("replanned", 0)
    replanReason = attr.get("replanReason", 0)

    flowControl = attr.get("flowControl", {})

    flowControl_acquireCount = flowControl.get("acquireCount", 0)
    flowControl_timeAcquiringMicros = flowControl.get("timeAcquiringMicros", 0)

    storage_data = attr.get("storage", {}).get("data", {})
    #disk
    storage_data_bytesRead = storage_data.get("bytesRead",0)
    storage_data_timeReadingMicros = storage_data.get("timeReadingMicros",0)

    storage_data_bytesWritten = storage_data.get("bytesWritten",0)
    storage_data_timeWritingMicros = storage_data.get("timeWritingMicros",0)

    #'timeWaitingMicros.cache
    #timeWaitingMicros.schemaLock
    #timeWaitingMicros.handleLock
    #execStats


    storage_data_bytesTotalDiskWR = storage_data_bytesRead + storage_data_bytesWritten
    storage_data_timeWRMicros = storage_data_timeReadingMicros + storage_data_timeWritingMicros



    #cache
    #storage.timeWaitingMicros.cache

    #

    writeConflicts = attr.get("writeConflicts", 0)
    command = attr.get("command", {})
    skip = command.get("skip", 0)
    limit = command.get("limit", 0)
    changestream = check_change_stream(log_entry)
    command_shape, in_counts = get_command_shape(command)
    if first_attribute_name=="getMore":
        orig_command = attr.get("originatingCommand", {})
        getMore=1
        command_shape, in_counts = get_command_shape(orig_command)
    count_of_in = len(in_counts)
    max_count_in = max(in_counts) if in_counts else 0
    sum_of_counts = sum(in_counts)
    if command_shape == 0:
        command_shape = "no_command"
    timestamp = log_entry.get("t", {}).get("$date")
    if timestamp:
        hour = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d %H:00:00')
        data.append([hour, namespace, 1, duration,planningTimeMicros, has_sort_stage, query_targeting, plan_summary,
                     command_shape,writeConflicts,skip,limit,appName,changestream,usedDisk,fromMultiPlanner,replanned,replanReason,keys_examined,docs_examined,nreturned,
                     cursorid,nBatches,numYields,totalOplogSlotDurationMicros,waitForWriteConcernDurationMillis,ninserted,
                     nMatched,nModified,nUpserted,ndeleted,keysInserted,keysDeleted,reslen,flowControl_acquireCount,flowControl_timeAcquiringMicros,
                     storage_data_bytesRead,storage_data_timeReadingMicros,storage_data_bytesWritten,storage_data_timeWritingMicros,storage_data_bytesTotalDiskWR,storage_data_timeWRMicros,
                     cmdType,count_of_in,max_count_in,sum_of_counts,getMore])


def get_command_shape(command):
    in_counts = []
    def replace_values(obj):
        if isinstance(obj, dict):
            new_obj = {}
            for k, v in obj.items():
                if k == "$in" and isinstance(v, list):
                    in_counts.append(len(v))
                    # Use a set to collect unique types
                    unique_types = {replace_values(i) for i in v}
                    new_obj[k] = list(unique_types)  # Convert set back to list
                else:
                    new_obj[k] = replace_values(v)
            return new_obj
        elif isinstance(obj, list):
            return [replace_values(i) for i in obj]
        elif isinstance(obj, int):
            return "int"
        elif isinstance(obj, float):
            return "float"
        elif isinstance(obj, str):
            return "string"
        elif isinstance(obj, bool):
            return "bool"
        else:
            return "other"
    def handle_pipeline(pipeline):
        new_pipeline = []
        for stage in pipeline:
            if isinstance(stage, dict):
                new_stage = {}
                for k, v in stage.items():
                    if k == "$group" and isinstance(v, dict):
                        new_stage[k] = {sub_k: replace_values(sub_v) if sub_k != "_id" else sub_v for sub_k, sub_v in v.items()}
                    elif k == "$lookup" and isinstance(v, dict):
                        new_stage[k] = {sub_k: replace_values(sub_v) if sub_k not in ["from", "localField", "foreignField", "as"] else sub_v for sub_k, sub_v in v.items()}
                    else:
                        new_stage[k] = replace_values(v)
                new_pipeline.append(new_stage)
            else:
                new_pipeline.append(replace_values(stage))
        return new_pipeline
    command_shape = replace_values(command)
    command_shape.pop('lsid', None)
    command_shape.pop('$clusterTime', None)

    keys = list(command_shape.keys())
    # Preserve specific keys in their original form if they are the first key
    for key in ["insert", "findAndModify", "update"]:
        if key in command_shape and keys.index(key) == 0:
            command_shape[key] = command[key]
    # Preserve other specific keys in their original form
    for key in ["collection", "aggregate", "find", "ordered", "$db"]:
        if key in command_shape:
            command_shape[key] = command[key]
    # Handle the pipeline separately
    if "pipeline" in command:
        command_shape["pipeline"] = handle_pipeline(command["pipeline"])
    return json.dumps(command_shape), in_counts


#---------------------------------------------------------------------------
# Plot Util :
def plot_stats(df, value_col, title, ylabel, xlabel='Time', output_file=None, columns = ['namespace'],report=None):
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
        plt.savefig(output_file, bbox_inches='tight')
        plt.savefig(f"{output_file}.svg", format="svg")
        print(f"Graph saved to {output_file}")
        if report:
           report.add_image(f"{output_file}.svg")
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
    plot_stats(df, 'slow_query_count', 'Number of Slow Queries per Hour per Namespace', 'Number of Slow Queries',
               output_file=file_name,report=report)

    # Plot total duration of slow queries per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Total Duration of Slow Queries per Hour per Namespace")
    file_name = f"{prefix}_total_duration_per_hour.png"
    plot_stats(df, 'total_duration', 'Total Duration of Slow Queries per Hour per Namespace', 'Total Duration (ms)',
               output_file=file_name,report=report)

#    tmp = df[df['plan_summary'].str.contains('COLLSCAN')]
    report.add_page()
    report.sub2Chapter_title("COLLSCAN Count per Hour per Namespace")
    file_name = f"{prefix}_COLLSCAN_per_hour.png"
    plot_stats(df_plan_summary[df_plan_summary['plan_summary'].str.contains('COLLSCAN')], 'slow_query_count', 'COLLSCAN Count per Hour per Namespace', 'COLLSCAN Count',
           output_file=file_name,columns = ['namespace','plan_summary'],report=report)

    # Plot hasSortStage count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Has Sort Stage Count per Hour per Namespace")
    file_name = f"{prefix}_has_sort_stage_per_hour.png"
    plot_stats(df, 'has_sort_stage', 'Has Sort Stage Count per Hour per Namespace', 'Has Sort Stage Count',
               output_file=file_name,report=report)

    # Plot writeConflicts count per hour per namespace
    report.add_page()
    report.sub2Chapter_title("write conflicts Count per Hour per Namespace")
    file_name = f"{prefix}_writeConflicts_per_hour.png"
    plot_stats(df, 'writeConflicts', 'write conflicts Count per Hour per Namespace', 'write conflicts Count',
               output_file=file_name,report=report)

    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Query Targeting per Hour per Namespace")
    file_name = f"{prefix}_query_targeting_per_hour.png"
    plot_stats(df, 'query_targeting', 'Query Targeting per Hour per Namespace', 'Query Targeting',
               output_file=file_name,report=report)


    # Plot query targeting per hour per namespace
    report.add_page()
    report.sub2Chapter_title("Skip per Hour per Namespace")
    file_name = f"{prefix}_skip_per_hour.png"
    plot_stats(df, 'sum_skip', 'total skip per Hour per Namespace', 'total skip',
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


    save_markdown(filtered_df, 'command_shape_collscan_stats.md', "collscan")
    report.subChapter_title("List of slow query shape")
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

#----------------------------------------------------------------------------------------
#  Main :
if __name__ == "__main__":
    first_option = sys.argv[1] if len(sys.argv) > 1 else None
    # Use the first_option in your Config class or elsewhere
    config = Config(first_option)
    report = Report(config)
    report.add_page()
    if config.USING_API :
       for process in config.PROCESSES_ID:
           addToReport(retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,process,config.OUTPUT_FILE_PATH),process,report)
    else :
       addToReport(extract_slow_queries(config.LOG_FILE_PATH, config.OUTPUT_FILE_PATH),config.LOG_FILE_PATH,report)

    report.write("slow_report")



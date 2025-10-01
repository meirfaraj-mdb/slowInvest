import os
import time

#  pip install -r requirements.txt

from sl_atlas.AtlasApi import AtlasApi
from sl_report.report import Report
from sl_async.slorch import extract_slow_queries_from_file
from sl_config.config import Config
from sl_plot.graphs import createAndInsertGraphs, plot_all_metricsForProcess
from sl_utils.utils import convertToHumanReadable
import concurrent.futures

import sys
import subprocess
from argparse import ArgumentParser
import logging


#---------------------------------------------------------------------------
# Plot Util :

#----------------------------------------------------------------------------------------
# display slow query
def process_row(index, row,report,columns):
    # Define a function to apply to each row
    report.addpage()

    cmdType=convertToHumanReadable('cmdType',row['cmdType'])
    namespace=convertToHumanReadable('namespace',row['namespace'])
    durationMillis_str=convertToHumanReadable('durationMillis',row['durationMillis_total'],True)
    slow_query_count=row['slow_query']
    bytesTotalDiskWR=convertToHumanReadable('bytesTotalDiskWR',row['storage_data_bytesTotalDiskWR_total'], True)
    bytesReslen=convertToHumanReadable('bytesReslen',row['bytesReslen_total'], True)
    report.sub4Chapter_title(
        f"{cmdType}"
        f"({namespace}) :"
        f" Time={durationMillis_str}, count={slow_query_count}"+
        (f" DISK={bytesTotalDiskWR}" if row['storage_data_bytesTotalDiskWR_total']>0 else "")+
        (f" RES={bytesReslen}" if row['bytesReslen_total']>0 else ""))


    report.add_json(row['command_shape'])
    report.addpage()
    report.table(row.drop(columns=['command_shape']), [col for col in columns if col != 'command_shape'])



def display_queries(reportTitle,report, df):
    if df is None or df.empty :
        return
    report.addpage()
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

        # Sort the grouped DataFrame by slow_query and average_duration in descending order
        sorted_grouped_df = app_group.sort_values(by=['durationMillis_total','slow_query'], ascending=[False, False])

        # Iterate over each row in the sorted grouped DataFrame
        for index, row in sorted_grouped_df.iterrows():
            process_row(index, row, report, sorted_grouped_df.columns)



#----------------------------------------------------------------------------------------
# report

def addToReport(result,prefix,report,config,process=None):
    report.chapter_title(f"Slow Query Report Summary : {prefix}")
    report.subChapter_title("General")
    report.chapter_body(f"Instance : {prefix}")
    if result.get("countOfSlow",0) == 0:
        report.chapter_body("No slow query!")
        report.addpage()
        return

    createAndInsertGraphs(config, prefix, report, result)

    if not(process is None) :
        plot_all_metricsForProcess(process, report,config,prefix)
    # Group by command shape and calculate statistics
    command_shape_stats = result["groupByCommandShape"].get("global",{})
    if len(command_shape_stats)>0 and command_shape_stats.shape[0]>0:
        addCommandShapAnalysis(command_shape_stats, config, report)

    ################## change stream
    command_shape_cs_stats = result["groupByCommandShapeChangeStream"].get("global",{})
    if (command_shape_cs_stats is not None) and len(command_shape_cs_stats)>0 and command_shape_cs_stats.shape[0]>0:
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
    display_queries("List of Collscan query shape", report, filtered_df)
    display_queries("List of remain hasSortStage query shape", report, sort_stage_df)
    display_queries("List of other query withConflict", report, with_conflict)
    ###################SKIP
    display_queries("List of other query with skip", report, has_skip)
    ###################bad $in
    display_queries("List of query with bad $in", report, has_badIn)
    ###################$Regex
    ###################disk
    display_queries("List of other query shape", report, without_badIn)


# TODO: has sort stage need fix


def display_cluster(config,report,cluster):
    report.display_cluster_table(cluster)

def atlas_retrieval_mode(config,report):
    atlasApi = AtlasApi(config)


    if config.ATLAS_RETRIEVAL_SCOPE == "project":
        print(f"Get project {config.GROUP_ID} composition....")
        start_time_comp = time.time()
        compositions = atlasApi.get_clusters_composition(config.GROUP_ID,full=config.GENERATE_INFRA_REPORT)
        end_time_comp = time.time()
        millis_str=convertToHumanReadable("Millis",(end_time_comp-start_time_comp)*1000,True)
        print(f"Received project {config.GROUP_ID} composition in {millis_str}")
        for cluster in compositions :
            generate_cluster_report(atlasApi, cluster, config, report)


    elif config.ATLAS_RETRIEVAL_SCOPE == "clusters":
        for cluster_name in config.CLUSTERS_NAME:
            print(f"Get cluster {cluster_name} composition....")
            start_time_comp = time.time()
            compositions = atlasApi.get_clusters_composition(config.GROUP_ID,cluster_name,full=config.GENERATE_INFRA_REPORT)
            end_time_comp = time.time()
            millis_str=convertToHumanReadable("Millis",(end_time_comp-start_time_comp)*1000,True)
            print(f"Received cluster {cluster_name} composition in {millis_str}")
            for cluster in compositions :
                generate_cluster_report(atlasApi, cluster, config, report)

    else: #processId
        for process in config.PROCESSES_ID:
            path=f"{config.OUTPUT_FILE_PATH}/process/{process}/"
            addToReport(atlasApi.retrieveLast24HSlowQueriesFromCluster(config.GROUP_ID,process,0,path,
                                                                       config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK),
                        process,
                        report,
                        config)

def remove_status_suffix(text):
    primary_suffix = '_PRIMARY'
    secondary_suffix = '_SECONDARY'
    if text.endswith(primary_suffix):
        return text[:-len(primary_suffix)]
    elif text.endswith(secondary_suffix):
        return text[:-len(secondary_suffix)]
    else:
        return text


def generate_cluster_report(atlasApi, cluster, config, report):
    if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        report = Report(config)
    atlasApi.save_cluster_result(cluster)
    processesShard = cluster.get("processes", {})
    with concurrent.futures.ProcessPoolExecutor() as pool:
        results={}
        for shard_num, processes in processesShard.items():
            name=cluster.get("name")
            if shard_num == "config":
                type_without_sfx=remove_status_suffix(processes.get("typeName"))
                hostname=processes.get("hostname")
                path=f"{config.OUTPUT_FILE_PATH}/{name}/{type_without_sfx}/{hostname}/"
                results[processes.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, processes.get("id", ""),shard_num,path,config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)
                continue
            primary = processes.get("primary", {})
            if len(primary) ==0:
                logging.error("no primary found")
                continue
            #atlasApi.get_database_composition_for_process(cluster, primary)
            type_without_sfx=remove_status_suffix(primary.get("typeName",""))
            hostname=primary.get("hostname")
            path=f"{config.OUTPUT_FILE_PATH}/{name}/{type_without_sfx}/{shard_num}/{hostname}/"
            results[primary.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, primary.get("id", ""),shard_num,
                                                               path,
                                                               config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)
            #atlasApi.get_database_composition_sizing_for_process(cluster, primary)

            others = processes.get("others", {})
            for proc in others:
                type_without_sfx=remove_status_suffix(proc.get("typeName"))
                hostname=proc.get("hostname")
                path=f"{config.OUTPUT_FILE_PATH}/{name}/{type_without_sfx}/{shard_num}/{hostname}/"
                results[proc.get("id", "")]=pool.submit(atlasApi.retrieveLast24HSlowQueriesFromCluster,config.GROUP_ID, proc.get("id", ""),shard_num,
                                                                   path,
                                                                   config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK)

        #pool.shutdown(wait=True)
        display_cluster(config, report, cluster)

        for shard_num, processes in processesShard.items():
            if shard_num == "config":
                processes_id=processes.get("id", "")
                type_without_sfx=remove_status_suffix(processes.get("typeName", ""))
                addToReport(
                    results[processes.get("id","")].result(),
                    f"{config.OUTPUT_FILE_PATH}/{shard_num}_{processes_id}_{type_without_sfx}",
                    report,
                    config,
                    processes)
                continue
            primary = processes.get("primary", {})
            #atlasApi.get_database_composition_for_process(cluster, primary)
            primary_id=primary.get("id", "")
            if len(primary) ==0:
                logging.error("no primary found")
                continue
            type_without_sfx=remove_status_suffix(primary.get("typeName", ""))
            addToReport(
                results[primary.get("id", "")].result(),
                f"{config.OUTPUT_FILE_PATH}/{shard_num}_{primary_id}_{type_without_sfx}",
                report,
                config,
                primary)
            atlasApi.get_database_composition_sizing_for_process(cluster, primary)

            others = processes.get("others", {})
            for proc in others:
                processes_id=proc.get("id", "")
                type_without_sfx=remove_status_suffix(proc.get("typeName", ""))
                addToReport(
                    results[proc.get("id", "")].result(),
                    f"{config.OUTPUT_FILE_PATH}/{shard_num}_{processes_id}_{type_without_sfx}",
                    report,
                    config,
                    proc)
    if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        name=cluster.get("name")
        report.write(f"{config.REPORT_FILE_PATH}/slow_report{name}")


def file_retrieval_mode(config,report):
    for file in config.LOGS_FILENAME:
        if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
            report = Report(config)
        addToReport(extract_slow_queries_from_file(f"{config.INPUT_PATH}/{file}", f"{config.OUTPUT_FILE_PATH}/slow_queries_{file}",
                                         config.MAX_CHUNK_SIZE,config.SAVE_BY_CHUNK),
                    f"{config.OUTPUT_FILE_PATH}/{file}",
                    report,
                    config)
        if config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
            report.write(f"{config.REPORT_FILE_PATH}/slow_report{file}")


def start_server():
    try:
        import django
        # Start the Django server
        subprocess.run(["python", "manage.py", "runserver", "0.0.0.0:8000"], cwd='sl_server')
        sys.exit(0)
    except ImportError:
        print("Django is not installed. Please install it using 'pip install Django'.")
        sys.exit(1)



def list_config_files(directory):
    """List all files in the given directory without their extension."""
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    return [os.path.splitext(f)[0] for f in files]


#----------------------------------------------------------------------------------------
#  Main :
if __name__ == "__main__":
    if sys.version_info[0] != 3 or sys.version_info[1] < 11:
        raise Exception('Requires python 3.11 best (tested only with 3.13.1)')

    if sys.version_info[1] < 13:
        logging.error(f"{sys.version_info} is not tested best to upgrade to 3.13.1")


    start_time_all=time.time()

    config_directory = "config/"
    config_choices = list_config_files(config_directory)

    parser = ArgumentParser(description='Analyse slow query')
    parser.add_argument('--server', action='store_true', help='Start the Django server')
    parser.add_argument('config_name', nargs='?', choices=config_choices, help='The name of the config to use from config/ directory')

    args = parser.parse_args()

    if args.server:
        start_server()

    first_option = sys.argv[1] if len(sys.argv) > 1 else None
    # Use the first_option in your Config class or elsewhere
    config = Config(first_option)
    report = Report(config)

    if config.retrieval_mode == "Atlas":
        atlas_retrieval_mode(config,report)
    elif config.retrieval_mode == "files":
        file_retrieval_mode(config,report)
    elif config.retrieval_mode == "OpsManager":
        print(f"Retrieval Mode: {config.retrieval_mode} not yet implemented use files mode instead.")
    else:
        raise ValueError(f"Unsupported retrieval_mode: {config.retrieval_mode}")

    if not config.GENERATE_ONE_PDF_PER_CLUSTER_FILE:
        report.write(f"{config.REPORT_FILE_PATH}/slow_report")
    end_time_all=time.time()
    millis_str=convertToHumanReadable("Millis",(end_time_all-start_time_all)*1000)
    print(f"work end in {millis_str}")

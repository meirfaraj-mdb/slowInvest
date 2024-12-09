from datetime import datetime
import pandas as pd
import json


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
    def replace_valuesAsStr(obj):
        res=replace_values(obj)
        if isinstance(res, str):
            return res
        return json.dumps(res)
    def replace_values(obj):
        if isinstance(obj, dict):
            new_obj = {}
            for k, v in obj.items():
                if k == "$in" and isinstance(v, list):
                    in_counts.append(len(v))
                    # Use a set to collect unique types
                    unique_types = {replace_valuesAsStr(i) for i in v}
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

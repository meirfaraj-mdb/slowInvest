import time
from datetime import datetime

import msgspec

encoder = msgspec.json.Encoder()
decoder = msgspec.json.Decoder()


class JsonAndText:
    def __init__(self,line):
        self.orig=line
        self.dtime=None
        self.dhour = None
        self.log_entry=None
        self.internal_decode()

    def internal_decode(self):
        self.log_entry = decoder.decode(self.orig)
        if self.log_entry.get("msg",None) == "Slow query":
            # Fully parse the JSON object since the condition is true
            timestamp = self.log_entry.get("t", {}).get("$date")
            if timestamp:
                self.dtime = datetime.fromisoformat(timestamp)
                self.dhour = self.dtime.strftime('%Y-%m-%d_%H')
                self.log_entry=extractSlowQueryInfos(self.log_entry)
                return
        self.clear()

    def set_line(self,line):
        self.orig=line

    def decode(self):
        return self.dtime,self.dhour,self.log_entry,self.orig

    def clear(self):
        self.orig=None
        self.dtime=None
        self.dhour = None
        self.log_entry=None


DF_COL = ['timestamp','hour', 'db', 'namespace', 'slow_query', 'durationMillis','planningTimeMicros', 'has_sort_stage', 'query_targeting',
          'plan_summary', 'command_shape', 'writeConflicts', 'skip', 'limit', 'appName', 'changestream', 'usedDisk',
          'fromMultiPlanner','replanned','replanReason',
          'keys_examined', 'docs_examined', 'nreturned', 'cursorid', 'nBatches', 'numYields',
          'totalOplogSlotDurationMicros', 'waitForWriteConcernDurationMillis', 'ninserted', 'nMatched', 'nModified',
          'nUpserted', 'ndeleted', 'keysInserted','keysDeleted', 'bytesReslen', 'flowControl_acquireCount', 'flowControl_timeAcquiringMicros',
          'storage_data_bytesRead', 'storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
          'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros',
          'storage_data_timeWaitingMicros_cache','storage_data_timeWaitingMicros_schemaLock','storage_data_timeWaitingMicros_handleLock',
          'cmdType','count_of_in','max_count_in','sum_of_counts_in','getMore']


def extractSlowQueryInfos(log_entry):
    start_time = time.time()
    # Extract relevant fields
    getMore=0
    # Access the 'attr' dictionary
    attr = log_entry.get('attr', {})

    namespace = attr.get("ns", "unknown")
    excluded_prefixes=('admin', 'local', 'config')
    if namespace.startswith(excluded_prefixes):
        return None


    # Get the 'type' value and convert it to lowercase
    type_value = attr.get('type', 'unknown').lower()

    # Access the 'command' dictionary
    command = attr.get('command', {})

    # Get the first attribute name in the 'command' dictionary
    first_attribute_name = next(iter(command), 'unknown')

    # Return the formatted string
    cmdType = f"{type_value}.{first_attribute_name}"


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

    timeWaitingMicros = storage_data.get("timeWaitingMicros", {})
    # cache
    storage_data_timeWaitingMicros_cache = timeWaitingMicros.get("cache",0)
    storage_data_timeWaitingMicros_schemaLock = timeWaitingMicros.get("schemaLock",0)
    storage_data_timeWaitingMicros_handleLock = timeWaitingMicros.get("handleLock",0)

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
    #command.get("readConcern",None)
    #readConcern:
    #{
    #level: string
    #provenance: string
    #}
    #

    #clientOperationKey:
# {
# $uuid: string
# }
# shardVersion:
# {
#     e:
#         {
# $oid: string
# }
# t:
# {
# $timestamp:
# {
#     t: int
#     i: int
# }
# }
# v:
# {
# $timestamp:
# {
#     t: int
#     i: int
# }
# }
# }
# databaseVersion:
# {
#     uuid:
#         {
# $uuid: string
# }
# timestamp:
# {
# $timestamp:
# {
#     t: int
#     i: int
# }
# }
# lastMod: int
# }
# $configTime:
# {
# $timestamp:
# {
#     t: int
#     i: int
# }
# }
# $topologyTime:
# {
# $timestamp:
# {
#     t: int
#     i: int
# }
# }
#     $client:
# {
#     driver:
#         {
#             name: string
#             version: string
#         }
#     os:
#         {
#             type: string
#             name: string
#             architecture: string
#             version: string
#         }
#     platform: string
#     application:
#         {
#             name: string
#         }
#     mongos:
#         {
#             host: string
#             client: string
#             version: string
#         }
# }
    command_shape, in_counts,db = get_command_shape(command,namespace)
    if first_attribute_name=="getMore":
        orig_command = attr.get("originatingCommand", {})
        getMore=1
        command_shape, in_counts,db = get_command_shape(orig_command,namespace)
    count_of_in = len(in_counts)
    max_count_in = max(in_counts) if in_counts else 0
    sum_of_counts = sum(in_counts)
    if command_shape == 0:
        command_shape = "no_command"

    if db.startswith(excluded_prefixes):
        return None

    timestamp = log_entry.get("t", {}).get("$date")

    if timestamp:
        hour = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d %H:00:00')
        return [timestamp,hour, db, namespace, 1, duration,planningTimeMicros, has_sort_stage, query_targeting, plan_summary,
                     command_shape,writeConflicts,skip,limit,appName,changestream,usedDisk,fromMultiPlanner,replanned,replanReason,keys_examined,docs_examined,nreturned,
                     cursorid,nBatches,numYields,totalOplogSlotDurationMicros,waitForWriteConcernDurationMillis,ninserted,
                     nMatched,nModified,nUpserted,ndeleted,keysInserted,keysDeleted,reslen,flowControl_acquireCount,flowControl_timeAcquiringMicros,
                     storage_data_bytesRead,storage_data_timeReadingMicros,storage_data_bytesWritten,storage_data_timeWritingMicros,storage_data_bytesTotalDiskWR,storage_data_timeWRMicros,
                     storage_data_timeWaitingMicros_cache,storage_data_timeWaitingMicros_schemaLock,storage_data_timeWaitingMicros_handleLock,
                     cmdType,count_of_in,max_count_in,sum_of_counts,getMore]

    return None


def get_command_shape(command,namespace):
    in_counts = []
    db=command.get("$db",namespace.split('.', 1))
    def replace_valuesAsStr(obj):
        res=replace_values(obj)
        if isinstance(res, str):
            return res
        return encoder.encode(res)
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
    command_shape.pop("readConcern",None)
    command_shape.pop("clientOperationKey",None)
    command_shape.pop("shardVersion",None)
    command_shape.pop("$timestamp",None)
    command_shape.pop("databaseVersion",None)
    command_shape.pop("$topologyTime",None)
    command_shape.pop("$configTime",None)
    command_shape.pop("$audit",None)
    command_shape.pop("$client",None)
    command_shape.pop("mayBypassWriteBlocking",None)
    keys = list(command_shape.keys())
    # Preserve specific keys in their original form if they are the first key
    for key in ["insert", "findAndModify", "update","delete"]:
        if key in command_shape and keys.index(key) == 0:
            command_shape[key] = command[key]
    # Preserve other specific keys in their original form
    for key in ["collection", "aggregate", "find", "ordered", "$db"]:
        if key in command_shape:
            command_shape[key] = command[key]
    # Handle the pipeline separately
    if "pipeline" in command:
        command_shape["pipeline"] = handle_pipeline(command["pipeline"])
    return encoder.encode(command_shape), in_counts, str(db)


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

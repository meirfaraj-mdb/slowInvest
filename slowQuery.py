from datetime import datetime, timedelta
import pandas as pd
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
import os.path
import msgspec
import gzip
from utils import convertToHumanReadable
import concurrent
encoder = msgspec.json.Encoder()
decoder = msgspec.json.Decoder()
DF_COL = ['timestamp','hour', 'db', 'namespace', 'slow_query_count', 'durationMillis','planningTimeMicros', 'has_sort_stage', 'query_targeting',
          'plan_summary', 'command_shape', 'writeConflicts', 'skip', 'limit', 'appName', 'changestream', 'usedDisk',
          'fromMultiPlanner','replanned','replanReason',
          'keys_examined', 'docs_examined', 'nreturned', 'cursorid', 'nBatches', 'numYields',
          'totalOplogSlotDurationMicros', 'waitForWriteConcernDurationMillis', 'ninserted', 'nMatched', 'nModified',
          'nUpserted', 'ndeleted', 'keysInserted','keysDeleted', 'bytesReslen', 'flowControl_acquireCount', 'flowControl_timeAcquiringMicros',
          'storage_data_bytesRead', 'storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
          'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros',
          'storage_data_timeWaitingMicros_cache','storage_data_timeWaitingMicros_schemaLock','storage_data_timeWaitingMicros_handleLock',
          'cmdType','count_of_in','max_count_in','sum_of_counts_in','getMore']




#------------------------------------------------------------------------------------

def createDirs(directory_path):
    # Create the directory along with any necessary parent directories
    try:
        os.makedirs(directory_path, exist_ok=True)
    except Exception as e:
        print(f"An error occurred: {e}")

def remove_extension(file_path):
    root, _ = os.path.splitext(file_path)
    if root.endswith(".log"):
        root, _ = os.path.splitext(root)
    return root


def distinct_values(series):
    # Drop NaN values and get unique values as a list
    flat_list = [item for sublist in series.dropna() for item in (sublist if isinstance(sublist, list) else [sublist])]
    unique_values = list(set(flat_list))
    return unique_values
    # Convert the list of unique values to a string separated by commas
    #return ', '.join(map(str, unique_values))
    # Other utilities
#def distinct_values(series):
#    return ', '.join(sorted(set(series)))

def minMaxAvgTtl(column_name):
    """Generate a dictionary of aggregation operations for a given column."""
    return {
        f'{column_name}_min': (column_name, 'min'),
        f'{column_name}_max': (column_name, 'max'),
        f'{column_name}_avg': (column_name, 'mean'),
        f'{column_name}_total': (column_name, 'sum'),
        f'{column_name}_count': (column_name, 'count')
    }

def getCommanShapeAggOp():
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
        'db': ('db', distinct_values),
        'namespace': ('namespace', distinct_values),
        'cmdType':('cmdType', distinct_values),
        'count_of_in':('count_of_in', distinct_values),
        'getMore':('getMore','sum')
    }

    # Add min, max, avg, total for each specified column

    for column in ['writeConflicts','durationMillis','planningTimeMicros', 'keys_examined', 'docs_examined', 'nreturned', 'query_targeting',
                   'nBatches', 'numYields','skip','limit', 'waitForWriteConcernDurationMillis','totalOplogSlotDurationMicros',
                   'ninserted', 'nMatched', 'nModified','nUpserted','ndeleted',
                   'keysInserted','keysDeleted','bytesReslen','flowControl_acquireCount','flowControl_timeAcquiringMicros',
                   'storage_data_bytesRead','storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
                   'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros',
                   'storage_data_timeWaitingMicros_cache','storage_data_timeWaitingMicros_schemaLock','storage_data_timeWaitingMicros_handleLock',
                   'max_count_in','sum_of_counts_in']:
        agg_operations.update(minMaxAvgTtl(column))
    return agg_operations


def groupbyCommandShape(df):
    return df.groupby('command_shape').agg(**getCommanShapeAggOp()).reset_index()

def makeSureLessThan24H(time):
    """
    Check if the given datetime is within the last 24 hours from the current time.
    Parameters:
    target_time (datetime): The datetime object to check.
    Returns:
    datetime or None: The target_time if it's within 24 hours, otherwise None.
    """
    current_time = datetime.now()
    time_difference = current_time - time

    if time_difference < timedelta(hours=24):
        return time
    else:
        return None

def init_result(file_path_base):
    result={}
    result["countOfSlow"]=0
    result["systemSkipped"]=0
    result["groupByCommandShape"]={}
    result["groupByCommandShapeChangeStream"]={}
    result["resume"]={}
    if os.path.isfile(f"{file_path_base}resume.json"):
        with open(f"{file_path_base}resume.json") as out_file:
            result["resume"]=decoder.decode(out_file.read())
        dtime=result["resume"].get("dtime",None)
        if dtime is not None:
            result["resume"]["dtime"]=datetime.fromisoformat(dtime)
    return result



class BufferedGzipWriter:
    def __init__(self, file_path, mode, buffer_size=4092):
        self.file_path = file_path
        self.buffer_size = buffer_size
        self._buffer = []
        self.gz_file = None
        self.curSize=0
        self.mode=mode
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def __enter__(self):
        # Open the gzip file and return the instance itself
        self.gz_file = gzip.open(self.file_path, self.mode, compresslevel=6)
        return self
    def write(self, data):
        self._buffer.append(data)
        self.curSize+=len(data)
        if self.curSize >= self.buffer_size:
            self.flush()
    def flush(self):
        if self._buffer and self.gz_file is not None:
            self.pool.submit(self.gz_file.writelines,self._buffer)
            self._buffer = []
            self.curSize=0

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Ensure flushing and closing of the gz_file
        self.close()
    def close(self):
        if self.gz_file is not None:
            self.flush()
            self.pool.shutdown(wait=True)
            self.gz_file.close()
            self.gz_file = None  # Ensure it can't be used after closing


# Function for extracting from File :
def extract_slow_queries(log_file_path, output_file_path, chunk_size=50000,save_by_chunk="none"):
    parquet_file_path_base=f"{remove_extension(output_file_path)}/"
    result=init_result(parquet_file_path_base)
    data = []
    start_time = time.time()

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    createDirs(parquet_file_path_base)
    it=0
    lastPrint=0
    lastHours = None
    dtime = result.get("resume",{}).get(None)
    # Detecting if the log file is gzipped
    if log_file_path.endswith('.gz'):
        log_open = lambda path: gzip.open(path, 'rt')
    else:
        log_open = lambda path: open(path, 'rt', buffering=4092)

    #if output_file_path.endswith('.gz'):

    with BufferedGzipWriter(f"{output_file_path}.gz","wt") as output_file:
        with log_open(log_file_path) as log_file:
            for line in log_file:
                try:
                    if len(line)<10:
                        continue
                    strings = ("Slow query")
                    find = False
                    if any(s in line for s in strings):
                       find=True
                    if not find :
                        continue
                    #log_entry = next(ijson.items(line, ''))
                    log_entry = decoder.decode(line)
                    if log_entry.get("msg",None) == "Slow query":
                        # Fully parse the JSON object since the condition is true
                        timestamp = log_entry.get("t", {}).get("$date")
                        if timestamp:
                            dtime = datetime.fromisoformat(timestamp)
                            day  = dtime.strftime('%Y%m%d')
                            dhour = dtime.strftime('%Y-%m-%d_%H')
                            if lastHours is None :
                                lastHours = dhour
                            if lastHours != dhour or len(data) >= chunk_size:
                                dumpAggregation=(lastHours != dhour)
                                lastHours = dhour
                                it+=1
                                pool.submit(append_to_parquet,data, parquet_file_path_base,dtime, it,save_by_chunk,dumpAggregation,result)
                                data = []  # Clear list to free memory
                                if result["countOfSlow"]-lastPrint>0 and result["countOfSlow"]-lastPrint>50000:
                                    lastPrint=result["countOfSlow"]
                                    end_time = time.time()
                                    elapsed_time_ms = (end_time - start_time) * 1000
                                    print(f"loaded {result["countOfSlow"]} slow queries in {convertToHumanReadable("Millis",elapsed_time_ms)}")
                        if extractSlowQueryInfos(data, log_entry):
                            output_file.write(line)
                        else:
                            result["systemSkipped"]+=1
                except msgspec.MsgspecError:
                    # Skip lines that are not valid JSON
                    continue
    # Handle any remaining data
    if data:
        it+=1
        pool.submit(append_to_parquet,data, parquet_file_path_base,dtime,it,save_by_chunk,True,result,True)
    pool.shutdown(wait=True)
    print(f"Extracted {result["countOfSlow"]} slow queries have been saved to {output_file_path} and {parquet_file_path_base}")
    return result

def concat_command(arr):
    # Concatenate the DataFrame; this will need care taken for recalculations
    if len(arr)==0:
        return None
    return pd.concat(arr, ignore_index=True)

def shape_aggA(concatenated):
    if concatenated is None:
        return None
    agg_operations=getCommanShapeAggOp()
    # Apply aggregation per group
    def aggregate_group(group):
        final_agg = {}
        final_agg["command_shape"]=group["command_shape"].iloc[0]
        for key, (column, operation) in agg_operations.items():
            if operation == 'sum':
                final_agg[key] = group[key].sum()
            elif operation == 'count':
                final_agg[key] = group[key].sum()  # Changed to correct count logic
            elif operation == 'mean':
                sum_column = f"{column}_total"
                count_column = f"{column}_count"
                final_agg[key] = group[sum_column].sum() / group[count_column].sum()
            elif operation == 'min':
                final_agg[key] = group[key].min()
            elif operation == 'max':
                final_agg[key] = group[key].max()
            elif operation == distinct_values:
                final_agg[key] = distinct_values(group[key])
            elif isinstance(operation, str):
                final_agg[key] = group[key].agg(operation)
            elif callable(operation):
                final_agg[key] = operation(group[key])
            else:
                print("key")
                continue
        return pd.Series(final_agg,name=group.name)
    # Perform the groupby operation with aggregations
    dfca = concatenated.groupby('command_shape').apply(aggregate_group)
    return dfca

def concat_command_shape_agg(df1,df2):
    if df1 is None:
        return df2
    if df1.shape[0]==0:
        return df2
    if df2.shape[0]==0:
        return df1
    # Concatenate the DataFrame; this will need care taken for recalculations
    concatenated = pd.concat([df1, df2], ignore_index=True)
    agg_operations=getCommanShapeAggOp()
    # Apply aggregation per group
    def aggregate_group(group):
        final_agg = {}
        final_agg["command_shape"]=group["command_shape"].iloc[0]
        for key, (column, operation) in agg_operations.items():
            if operation == 'sum':
                final_agg[key] = group[key].sum()
            elif operation == 'count':
                final_agg[key] = group[key].count()  # Changed to correct count logic
            elif operation == 'mean':
                sum_column = f"{column}_total"
                count_column = f"{column}_count"
                final_agg[key] = group[sum_column].sum() / group[count_column].sum()
            elif operation == 'min':
                final_agg[key] = group[key].min()
            elif operation == 'max':
                final_agg[key] = group[key].max()
            elif operation == distinct_values:
                final_agg[key] = distinct_values(group[key])
            elif isinstance(operation, str):
                final_agg[key] = group[key].agg(operation)
            elif callable(operation):
                final_agg[key] = operation(group[key])
            else:
                print("key")
                continue
        return pd.Series(final_agg,name=group.name)
    # Perform the groupby operation with aggregations
    dfca = concatenated.groupby('command_shape').apply(aggregate_group)
    return dfca


def append_to_parquet(data, file_path_base,dtime,id,save_by_chunk,dumpAggregation,result,saveAll=False,
                      generate_orig_only=False):
    day  = dtime.strftime('%Y%m%d')
    hour = dtime.strftime('%H')
    dhour = dtime.strftime('%Y-%m-%d_%H')
    result["countOfSlow"]+=len(data)
    if saveAll:
        result["resume"]["id"]=id
        result["resume"]["dtime"]=dtime.isoformat()
        with open(f"{file_path_base}resume.json", "w") as out_file:
            encoder.encode(result["resume"])
    file_path_base=f"{file_path_base}{day}/"
    file_path=f"{file_path_base}{hour}/"
    createDirs(file_path)
    df_chunk = pd.DataFrame(data, columns=DF_COL)
    if not generate_orig_only :
       updateCommandShapeGroupHour(df_chunk[df_chunk['changestream'] == False], dhour, result, "groupByCommandShape")
       updateCommandShapeGroupHour(df_chunk[df_chunk['changestream'] == True], dhour, result, "groupByCommandShapeChangeStream")


    if save_by_chunk == "parquet":
        write_parquet(df_chunk, f"{file_path}/{id}_orig.parquet")
        if (dumpAggregation or saveAll) and (not generate_orig_only) :
            write_parquet(result["groupByCommandShape"][dhour], f"{file_path}/{id}_groupByShape.parquet")
        if saveAll and (not generate_orig_only):
            updateCommandShapeGroupGlobal(result)
            write_parquet(result["groupByCommandShape"]["global"], f"{file_path_base}/{id}_groupByShapeAll.parquet")
    elif save_by_chunk == "json":
        #split for compact json
        df_chunk.to_json(f"{file_path}/{id}_orig.json", orient = 'records', compression = 'infer')
        if (dumpAggregation or saveAll) and (not generate_orig_only):
            result["groupByCommandShape"][dhour].to_json(f"{file_path}/{id}_groupByShape.json", orient = 'records', compression = 'infer')
        if saveAll and (not generate_orig_only):
            updateCommandShapeGroupGlobal(result)
            result["groupByCommandShape"]["global"].to_json(f"{file_path_base}/{id}_groupByShapeAll.json", orient = 'records', compression = 'infer')


def updateCommandShapeGroupHour(df_withoutChangestream, hour, result, type):
    command_shape_stats = groupbyCommandShape(df_withoutChangestream)
    result[type][hour] = concat_command_shape_agg(
        result[type].get(hour, None), command_shape_stats)
    return True

def updateCommandShapeGroupGlobal(result):
    for type in ["groupByCommandShape","groupByCommandShapeChangeStream"]:
        array = []
        keys = []
        for key,value in result[type].items():
            if value is not None and value.shape[0] > 0:
                value["hour"]=key
                array.append(value)
            keys.append(key)
        for key in keys:
            del result[type][key]
        result[type]["hours"] = concat_command(array)
        result[type]["global"] = shape_aggA(result[type]["hours"])
    return True


def write_parquet(df_chunk, path):
    for col in df_chunk.select_dtypes(include=['object']).columns:
        df_chunk[col] = df_chunk[col].astype(str)
    table = pa.Table.from_pandas(df_chunk)
    pq_writer = pq.ParquetWriter(path, table.schema, compression='SNAPPY')
    pq_writer.write_table(table)
    pq_writer.close()


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

def extractSlowQueryInfos(data, log_entry):
    # Extract relevant fields
    getMore=0
    # Access the 'attr' dictionary
    attr = log_entry.get('attr', {})

    namespace = attr.get("ns", "unknown")
    excluded_prefixes=('admin', 'local', 'config')
    if namespace.startswith(excluded_prefixes):
        return False


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
        return False

    timestamp = log_entry.get("t", {}).get("$date")
    if timestamp:
        hour = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d %H:00:00')
        data.append([timestamp,hour, db, namespace, 1, duration,planningTimeMicros, has_sort_stage, query_targeting, plan_summary,
                     command_shape,writeConflicts,skip,limit,appName,changestream,usedDisk,fromMultiPlanner,replanned,replanReason,keys_examined,docs_examined,nreturned,
                     cursorid,nBatches,numYields,totalOplogSlotDurationMicros,waitForWriteConcernDurationMillis,ninserted,
                     nMatched,nModified,nUpserted,ndeleted,keysInserted,keysDeleted,reslen,flowControl_acquireCount,flowControl_timeAcquiringMicros,
                     storage_data_bytesRead,storage_data_timeReadingMicros,storage_data_bytesWritten,storage_data_timeWritingMicros,storage_data_bytesTotalDiskWR,storage_data_timeWRMicros,
                     storage_data_timeWaitingMicros_cache,storage_data_timeWaitingMicros_schemaLock,storage_data_timeWaitingMicros_handleLock,
                     cmdType,count_of_in,max_count_in,sum_of_counts,getMore])
        return True
    return False


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

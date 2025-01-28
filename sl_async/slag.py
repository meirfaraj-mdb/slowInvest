import logging
import time
from datetime import datetime, timedelta

import pandas as pd

from sl_async.st_parquet import write_parquet
from sl_json.json import encoder, DF_COL
from sl_utils.utils import createDirs


#import dask.dataframe as dd


def distinct_values(series):
    # Drop NaN values and get unique values as a list
    flat_list = [item for sublist in series.dropna() for item in (sublist if isinstance(sublist, list) else [sublist])]
    unique_values = list(set(flat_list))
    if 0 in unique_values:
        unique_values.remove(0)
    return unique_values
    # Convert the list of unique values to a string separated by commas
    #return ', '.join(map(str, unique_values))
    # Other utilities


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
        'slow_query': ('slow_query', 'sum'),
        'app_name': ('appName', distinct_values),
        'db': ('db', distinct_values),
        'source':('source', distinct_values),
        'shard': ('shard', distinct_values),
        'namespace': ('namespace', distinct_values),
        'cmdType':('cmdType', distinct_values),
        'readPreference':('readPreference', distinct_values),
        'has_sort_stage': ('has_sort_stage', lambda x: x.mode().iloc[0] if not x.mode().empty else False),
        'usedDisk': ('usedDisk', distinct_values),
        'plan_summary': ('plan_summary', distinct_values),
        'fromMultiPlanner': ('fromMultiPlanner', distinct_values),
        'replanned':('replanned', distinct_values),
        'replanReason':('replanReason', distinct_values),
        'count_of_in':('count_of_in', distinct_values),
        'getMore':('getMore','sum')
    }

    # Add min, max, avg, total for each specified column

    for column in ['writeConflicts','workingMillis','durationMillis','cpuNanos','planningTimeMicros', 'keys_examined', 'docs_examined', 'nreturned', 'query_targeting',
                   'nBatches', 'numYields','skip','limit', 'waitForWriteConcernDurationMillis','totalOplogSlotDurationMicros',
                   'ninserted', 'nMatched', 'nModified','nUpserted','ndeleted',
                   'keysInserted','keysDeleted','bytesReslen','flowControl_acquireCount','flowControl_timeAcquiringMicros',
                   'storage_data_bytesRead','storage_data_timeReadingMicros','storage_data_bytesWritten','storage_data_timeWritingMicros',
                   'storage_data_bytesTotalDiskWR','storage_data_timeWRMicros',
                   'storage_data_timeWaitingMicros_cache','storage_data_timeWaitingMicros_schemaLock','storage_data_timeWaitingMicros_handleLock',
                   'max_count_in','sum_of_counts_in']:
        agg_operations.update(minMaxAvgTtl(column))
    return agg_operations

total_algo_stand = 0
total_algo_dd = 0
# df['command_shape'].astype('category')
# Use NumPy Operations: If possible, refactor the aggregation to use NumPy operations, which are generally faster as they are implemented in C.
#
def groupbyCommandShape(df):
    global total_algo_stand
    global total_algo_dd
    #return df.groupby('command_shape').agg(**getCommanShapeAggOp()).reset_index()
    start = time.time_ns()
    result = df.groupby('command_shape').agg(**getCommanShapeAggOp()).reset_index()
    end = time.time_ns()
    total_algo_stand+=(end-start)

#    start = time.time_ns()
#    ddf = dd.from_pandas(df, npartitions=4)
#    agg_result = ddf.groupby('command_shape').agg(**getCommanShapeAggOp()).compute().reset_index()
#    end = time.time_ns()
#    total_algo_dd+=(end-start)
    return result

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
                logging.warning("key")
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
                final_agg[key] = group[key].sum()
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
                logging.warning("key")
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
        winner = "dask"
        if total_algo_stand < total_algo_dd :
            winner = "stand"
        elif total_algo_stand == total_algo_dd:
            winner = "none"
        logging.info(f"winner={winner} stand={total_algo_stand}ns and dd={total_algo_dd}ns")
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
    return True


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


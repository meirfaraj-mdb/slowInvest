import requests
from requests.auth import HTTPDigestAuth
import concurrent
from slowQuery import *
import re
import time

def convert_list_to_dict(databases):
    """
    Converts a list of database names into a dictionary with each database name
    as a key and -1 as its corresponding value.

    Parameters:
        databases (list): A list of database names.

    Returns:
        dict: A dictionary with database names as keys and -1 as values.
    """
    return {database: -1 for database in databases}


class AtlasApi():
    def __init__(self,config):
        self.config=config
        self.PUBLIC_KEY  = self.config.PUBLIC_KEY
        self.PRIVATE_KEY = self.config.PRIVATE_KEY
        self.countOfRequest = 0

    #------------------------------------------------------------------------------------
    # Function for extracting from Api
    def atlas_request(self, op, fpath, fdate, arg):
        apiBaseURL = '/api/atlas/v2'
        url = f"https://cloud.mongodb.com{apiBaseURL}{fpath}"
        headers = {
            'Accept': f"application/vnd.atlas.{fdate}+json",
            'Content-Type': f"application/vnd.atlas.{fdate}+json"
        }
        try:
            response = requests.get(
                url,
                params=arg,
                auth=HTTPDigestAuth(self.PUBLIC_KEY, self.PRIVATE_KEY),
                headers=headers
            )
            # Check if the response was successful
            if response.status_code != 200:
                print(f"Error in {op}: {response.status_code} - {response.text}")
                response.raise_for_status()  # This will raise an HTTPError for bad responses
            #print(f"{op} response: {response.text}")
            return json.loads(response.text)
        except requests.exceptions.RequestException as e:
            # Catch any request-related errors
            print(f"Request failed for {op}: {e}")
            raise

    # retrieve slow queries
    def retrieveLast24HSlowQueriesFromCluster(self,groupId,processId, output_file_path, chunk_size=50000,save_by_chunk="none"):
        parquet_file_path_base=f"{remove_extension(output_file_path)}/"
        createDirs(parquet_file_path_base)
        result=init_result(parquet_file_path_base)
        data = []
        start_time = time.time()
        last_count = -1
        sl_output_file_path = f"{output_file_path}/slow_queries_{groupId}_{processId}.log"
        it= result["resume"].get("id",0)
        lastHours = None
        dtime = result["resume"].get("dtime",None)
        with open(sl_output_file_path, 'w', encoding='utf-8') as output_file:
            while last_count <0 or last_count>=15000 :
                path=f"/groups/{groupId}/processes/{processId}/performanceAdvisor/slowQueryLogs"
                arg={}
                if not (dtime is None):
                    since=str(int(time.mktime(dtime.timetuple())*1000))
                    print(f"executing slowQuery {processId} : since : {since}")
                    arg={"since": since}
                resp=self.atlas_request('SlowQueries', path, '2023-01-01', arg)
                last_count=len(resp['slowQueries'])
                for entry in resp['slowQueries']:
                    line = entry['line']
                    try:
                        log_entry = json.loads(line)
                        if log_entry.get("msg") == "Slow query":
                            timestamp = log_entry.get("t", {}).get("$date")
                            if timestamp:
                                dtime = datetime.fromisoformat(timestamp)
                                day  = dtime.strftime('%Y%m%d')
                                dhour = dtime.strftime('%Y-%m-%d_%H')
                                if lastHours is None :
                                    lastHours = dhour
                                if not (lastHours == dhour) or len(data) >= chunk_size:
                                    dumpAggregation=not (lastHours == dhour)
                                    lastHours = dhour
                                    it+=1
                                    append_to_parquet(data, parquet_file_path_base,dtime, it,save_by_chunk,dumpAggregation,result)
                                    if result["countOfSlow"]%200000==0:
                                        end_time = time.time()
                                        elapsed_time_ms = (end_time - start_time) * 1000
                                        print(f"loaded {result["countOfSlow"]} slow queries in {elapsed_time_ms} ms")
                                    data = []  # Clear list to free memory
                                if extractSlowQueryInfos(data, log_entry):
                                    output_file.write(line)
                                else:
                                    result["systemSkipped"]+=1
                    except json.JSONDecodeError:
                        # Skip lines that are not valid JSON
                        continue

                # Handle any remaining data
                if data:
                    it+=1
                    append_to_parquet(data, parquet_file_path_base,dtime,it,save_by_chunk,True,result,True)
        print(f"Extracted {result["countOfSlow"]} slow queries have been saved to {output_file_path} and {parquet_file_path_base}")
        return result

    def listAllProject(self):
        resp=self.atlas_request("GetAllProject",
                              "/groups",
                                "2023-01-01",
                                {})
        return resp

    def getOneProject(self,groupId):
        resp=self.atlas_request("GetOneProject",
                                f"/groups/{groupId}",
                                "2023-01-01",
                                {})
        return resp

# {
#     "links": [
#         {
#             "href": "https://cloud.mongodb.com/api/atlas",
#             "rel": "self"
#         }
#     ],
#     "results": [
#         {
#             "_id": "32b6e34b3d91647abb20e7b8",
#             "clusterName": "string",
#             "collName": "string",
#             "collectionType": "TIMESERIES",
#             "criteria": {
#                 "type": "DATE"
#             },
#             "dataExpirationRule": {
#                 "expireAfterDays": 7
#             },
#             "dataProcessRegion": {
#                 "cloudProvider": "AWS"
#             },
#             "dataSetName": "string",
#             "dbName": "string",
#             "groupId": "32b6e34b3d91647abb20e7b8",
#             "partitionFields": [
#                 {
#                     "fieldName": "string",
#                     "fieldType": "date",
#                     "order": 0
#                 }
#             ],
#             "paused": true,
#             "schedule": {
#                 "type": "DEFAULT"
#             },
#             "state": "PENDING"
#         }
#     ],
#     "totalCount": 0
# }
    def getAllOnlineArchiveForOneCluster(self,groupId,clusterName):
        resp=self.atlas_request("GetOnlineArchiveForCluster",
                                f"/groups/{groupId}/clusters/{clusterName}/onlineArchives",
                                "2023-01-01",
                                {})
        return resp

# [
#     {
#         "collectionName": "string",
#         "database": "string",
#         "indexID": "32b6e34b3d91647abb20e7b8",
#         "latestDefinition": {
#             "numPartitions": 1
#         },
#         "latestDefinitionVersion": {
#             "createdAt": "2019-08-24T14:15:22Z",
#             "version": 0
#         },
#         "name": "string",
#         "queryable": true,
#         "status": "DELETING",
#         "statusDetail": [
#             {
#                 "hostname": "string",
#                 "mainIndex": {
#                     "definition": {
#                         "numPartitions": 1
#                     },
#                     "definitionVersion": {
#                         "createdAt": "2019-08-24T14:15:22Z",
#                         "version": 0
#                     },
#                     "message": "string",
#                     "queryable": true,
#                     "status": "DELETING"
#                 },
#                 "queryable": true,
#                 "stagedIndex": {
#                     "definition": {
#                         "numPartitions": 1
#                     },
#                     "definitionVersion": {
#                         "createdAt": "2019-08-24T14:15:22Z",
#                         "version": 0
#                     },
#                     "message": "string",
#                     "queryable": true,
#                     "status": "DELETING"
#                 },
#                 "status": "DELETING"
#             }
#         ],
#         "type": "search"
#     }
# ]
    def getAllAtlasSearchIndexForOneCluster(self,groupId,clusterName):
        resp=self.atlas_request("getAllAtlasSearchIndexForOneCluster",
                            f"/groups/{groupId}/clusters/{clusterName}/search/indexes",
                            "2024-05-30",
                            {})
        return resp


# {
#     "shapes": [
#         {
#             "avgMs": 0,
#             "count": 0,
#             "id": "stringstringstringstring",
#             "inefficiencyScore": 0,
#             "namespace": "string",
#             "operations": [
#                 {
#                     "predicates": [
#                         {}
#                     ],
#                     "stats": {
#                         "ms": 0,
#                         "nReturned": 0,
#                         "nScanned": 0,
#                         "ts": 0
#                     }
#                 }
#             ]
#         }
#     ],
#     "suggestedIndexes": [
#         {
#             "avgObjSize": 0,
#             "id": "stringstringstringstring",
#             "impact": [
#                 "stringstringstringstring"
#             ],
#             "index": [
#                 {
#                     "property1": 1,
#                     "property2": 1
#                 }
#             ],
#             "namespace": "string",
#             "weight": 0
#         }
#     ]
# }
    def getPerformanceAdvisorSuggestedIndexes(self,groupId,clusterName,args={}):
        try:
            resp=self.atlas_request("getPerformanceAdvisorSuggestedIndexes",
                                f"/groups/{groupId}/clusters/{clusterName}/performanceAdvisor/suggestedIndexes",
                                "2024-08-05",
                                args)
            return resp
        except requests.exceptions.RequestException as e:
            return {
                "shapes": [],
                "suggestedIndexes": [],
                "error": f"{e} - cannot get suggested index"
            }


# {
#     "authorizedEmail": "user@example.com",
#     "authorizedUserFirstName": "string",
#     "authorizedUserLastName": "string",
#     "copyProtectionEnabled": false,
#     "encryptionAtRestEnabled": false,
#     "onDemandPolicyItem": {
#         "frequencyInterval": 0,
#         "frequencyType": "ondemand",
#         "id": "stringstringstringstring",
#         "retentionUnit": "days",
#         "retentionValue": 0
#     },
#     "pitEnabled": false,
#     "projectId": "32b6e34b3d91647abb20e7b8",
#     "restoreWindowDays": 0,
#     "scheduledPolicyItems": [
#         {
#             "frequencyInterval": 1,
#             "frequencyType": "daily",
#             "id": "stringstringstringstring",
#             "retentionUnit": "days",
#             "retentionValue": 0
#         }
#     ],
#     "state": "ACTIVE",
#     "updatedDate": "2019-08-24T14:15:22Z",
#     "updatedUser": "user@example.com"
# }
    def getBackupCompliance(self,groupId,args={}):
        resp=self.atlas_request("getBackupCompliance",
                                f"/groups/{groupId}/backupCompliancePolicy",
                                "2023-10-01",
                                args)
        return resp


# {
#     "links": [
#         {
#             "href": "https://cloud.mongodb.com/api/atlas",
#             "rel": "self"
#         }
#     ],
#     "results": [
#         {
#             "cloudProvider": "AWS",
#             "copyRegions": [
#                 "string"
#             ],
#             "createdAt": "2019-08-24T14:15:22Z",
#             "description": "string",
#             "expiresAt": "2019-08-24T14:15:22Z",
#             "frequencyType": "hourly",
#             "id": "32b6e34b3d91647abb20e7b8",
#             "links": [
#                 {
#                     "href": "https://cloud.mongodb.com/api/atlas",
#                     "rel": "self"
#                 }
#             ],
#             "masterKeyUUID": "72659f08-8b3c-4913-bb4e-a8a68e036502",
#             "mongodVersion": "string",
#             "policyItems": [
#                 "32b6e34b3d91647abb20e7b8"
#             ],
#             "replicaSetName": "string",
#             "snapshotType": "onDemand",
#             "status": "queued",
#             "storageSizeBytes": 0,
#             "type": "replicaSet"
#         }
#     ],
#     "totalCount": 0
# }
# Replica Set
# {
#     "links": [
#         {
#             "href": "https://cloud.mongodb.com/api/atlas",
#             "rel": "self"
#         }
#     ],
#     "results": [
#         {
#             "configServerType": "EMBEDDED",
#             "createdAt": "2019-08-24T14:15:22Z",
#             "description": "string",
#             "expiresAt": "2019-08-24T14:15:22Z",
#             "frequencyType": "hourly",
#             "id": "32b6e34b3d91647abb20e7b8",
#             "links": [
#                 {
#                     "href": "https://cloud.mongodb.com/api/atlas",
#                     "rel": "self"
#                 }
#             ],
#             "masterKeyUUID": "72659f08-8b3c-4913-bb4e-a8a68e036502",
#             "members": [
#                 {
#                     "cloudProvider": "AWS",
#                     "id": "32b6e34b3d91647abb20e7b8",
#                     "replicaSetName": "string"
#                 }
#             ],
#             "mongodVersion": "string",
#             "policyItems": [
#                 "32b6e34b3d91647abb20e7b8"
#             ],
#             "snapshotIds": [
#                 "32b6e34b3d91647abb20e7b8"
#             ],
#             "snapshotType": "onDemand",
#             "status": "queued",
#             "storageSizeBytes": 0,
#             "type": "replicaSet"
#         }
#     ],
#     "totalCount": 0
# }
# Sharded
# https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters
    def listAllBackupSnapshotForCluster(self,groupId,clusterName,clusterType):
        if "SHARDED" in clusterType:
            resp=self.atlas_request("GetAllCluster",
                                    f"/groups/{groupId}/clusters/{clusterName}/backup/snapshots/shardedClusters",
                                    "2023-01-01",
                                    {})
            return resp
        resp=self.atlas_request("GetAllCluster",
                        f"/groups/{groupId}/clusters/{clusterName}/backup/snapshots",
                        "2023-01-01",
                        {})
        return resp


    def listAllProjectClusters(self,group_id):
        resp=self.atlas_request("GetAllCluster",
                                f"/groups/{group_id}/clusters",
                                "2024-08-05",
                                {})
        return resp


    def getOneCluster(self,group_id,cluster_name):
        resp=self.atlas_request("GetAllCluster",
                                f"/groups/{group_id}/clusters/{cluster_name}",
                                "2024-10-23",
                                {})
        return resp

    def getAllProcessesForProject(self,group_id):
        resp = self.atlas_request("GetAllProcesses",
                                  f"/groups/{group_id}/processes",
                                  "2023-01-01",
                                  {})
        return resp

    # https://www.mongodb.com/docs/atlas/reference/api-resources-spec/v2/#tag/Clusters/operation/getClusterAdvancedConfiguration
    def getAdvancedConfigurationForOneCluster(self,group_id,cluster_name):
        resp=self.atlas_request("getAdvancedConfigurationForOneCluster",
                                f"/groups/{group_id}/clusters/{cluster_name}/processArgs",
                                "2024-08-05",
                                {})
        return resp



    def get_database_for_process(self, group_id, process_id):
        all_results = []
        page_num = 1
        total_count = None
        while True:
            resp = self.atlas_request(
                "get_database_for_process",
             f"/groups/{group_id}/processes/{process_id}/databases",
                "2023-01-01",
                {
                    'itemsPerPage': '500',
                    'pageNum': str(page_num),
                }
            )

            # Assuming 'results' is the list of returned documents and 'totalCount' is the total number of documents
            all_results.extend(resp.get('results', []))
            # If total_count is not set, initialize it
            if total_count is None:
                total_count = resp.get('totalCount', len(all_results))

            # Break if we've collected all results
            if len(all_results) >= total_count:
                break

            # Increment page number to get the next set of results
            page_num += 1
        resp = {}
        resp['results']=all_results
        resp['totalCount']=total_count
        return resp

    def calculate_instance_composition(self, cluster):
        instance_composition = {}
        if cluster['clusterType'] == "SHARDED" and not (cluster["configServerType"]=="EMBEDDED") :
            instance_composition["M30"] = 3
        for cspec in cluster['replicationSpecs']:
            for spec in cspec['regionConfigs']:
                for key, value in spec.items():
                    if key.endswith('Specs'):
                        instance_size = value['instanceSize']
                        node_count = value['nodeCount']
                        if node_count == 0:
                            continue
                        if instance_size in instance_composition:
                           instance_composition[instance_size] += node_count
                        else:
                            instance_composition[instance_size] = node_count
        cluster["instance_composition"] = instance_composition
        return cluster

# missing
    def get_clusters_composition(self,group_id=None,cluster_name=None,full=True):
        result=[]

        #Group_id None not yet supported
        #project_ids = []
        #    for project in projects_response['results']:
        #        project_ids.append(project['id'])
        #        project_id_to_name[project['id']] = project['name']
        #    for project_id in project_ids:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        processes_f = pool.submit(self.getAllProcessesForProject,group_id)


        if cluster_name is None:
            clusters = self.listAllProjectClusters(group_id)
        else:
            clusters = {'results':[ self.getOneCluster(group_id,cluster_name)]}

        cluster_name_to_inf = {}
        processes_by_cluster = {}
        for cluster in clusters.get('results',[]):
            cluster_name=cluster.get('name')
            cluster["futur"]={}
            if full:
                cluster["futur"]["performanceAdvisorSuggestedIndexes"] = pool.submit(self.getPerformanceAdvisorSuggestedIndexes,group_id,cluster_name)
                cluster["futur"]["onlineArchiveForOneCluster"] = pool.submit(self.getAllOnlineArchiveForOneCluster,group_id,cluster_name)
                cluster["futur"]["backupCompliance"] = pool.submit(self.getBackupCompliance,group_id)
                cluster["futur"]["backup"] = pool.submit(self.listAllBackupSnapshotForCluster,group_id,cluster_name,cluster["clusterType"])
                cluster["futur"]["advancedConfiguration"] = pool.submit(self.getAdvancedConfigurationForOneCluster,group_id, cluster_name)

            replicationSpecs = cluster.get('replicationSpecs',None)
            providersSet = set()
            regionSet = set()
            if replicationSpecs is not None:
                for spec in replicationSpecs:
                    regionConfigs=spec.get('regionConfigs',[])
                    for regionConfig in regionConfigs:
                        providersSet.add(regionConfig.get('providerName',''))
                        regionSet.add(f"{regionConfig.get('providerName','')}_{regionConfig.get('regionName','')}")

            cluster["providers"]=providersSet
            cluster["providers_count"]=len(providersSet)
            cluster["regions"]=regionSet
            cluster["regions_count"]=len(regionSet)

            cluster_name_to_inf[cluster_name.lower()] = cluster
            if cluster.get('paused',False):
                print(f"{cluster_name} is paused skip instance composition and processes!")
                result.append(cluster)
                continue
            cluster=self.calculate_instance_composition(cluster)
            result.append(cluster)
        processes = processes_f.result().get('results',[])
        for process in processes:
            user_alias = process['userAlias']
            pattern_config = r"(.+)-(config)-(\d+)-(\d+)"
            match_config = re.match(pattern_config, user_alias)
            if match_config:
                cluster_name = match_config.group(1)
                shard_number = int(match_config.group(3))
                if cluster_name_to_inf.get(cluster_name.lower(),None) is None:
                    continue
                configServerType = cluster_name_to_inf.get(cluster_name.lower(),{}).get('configServerType',None)
                if cluster_name not in processes_by_cluster:
                    processes_by_cluster[cluster_name] = {}
                processes_by_cluster[cluster_name]["config"] = process
                if configServerType == 'EMBEDDED':
                    replicationSpecs = cluster_name_to_inf.get(cluster_name.lower(),{}).get('replicationSpecs',[])
                    shard_number = len(replicationSpecs)-1
                    #print(f"{shard_number}")
                    if cluster_name not in processes_by_cluster:
                        processes_by_cluster[cluster_name] = {}
                    if shard_number not in processes_by_cluster[cluster_name]:
                        processes_by_cluster[cluster_name][shard_number]={}
                    if "PRIMARY" in process['typeName']:
                        processes_by_cluster[cluster_name][shard_number]["primary"] = process
                    else:
                        if "others" not in processes_by_cluster[cluster_name][shard_number]:
                            processes_by_cluster[cluster_name][shard_number]["others"]=[]
                        processes_by_cluster[cluster_name][shard_number]["others"].append(process)
                    cluster_name_to_inf[cluster_name.lower()]["processes"]=processes_by_cluster[cluster_name]
            else:
                pattern = r"(.+)-(shard)-(\d+)-(\d+)"
                match = re.match(pattern, user_alias)
                if match:
                    cluster_name = match.group(1)
                    if cluster_name_to_inf.get(cluster_name.lower(),None) is None:
                        continue
                    shard_number = int(match.group(3))
                    if cluster_name not in processes_by_cluster:
                        processes_by_cluster[cluster_name] = {}
                    if shard_number not in processes_by_cluster[cluster_name]:
                        processes_by_cluster[cluster_name][shard_number]={}
                    if "PRIMARY" in process['typeName']:
                        processes_by_cluster[cluster_name][shard_number]["primary"] = process
                    else:
                        if "others" not in processes_by_cluster[cluster_name][shard_number]:
                            processes_by_cluster[cluster_name][shard_number]["others"]=[]
                        processes_by_cluster[cluster_name][shard_number]["others"].append(process)
                    cluster_name_to_inf[cluster_name.lower()]["processes"]=processes_by_cluster[cluster_name]
        return result

    def get_database_composition_for_process(self,cluster,process):
        #         for cluster_name, shards in processes_by_cluster.items():
        #             to_write = cluster_name_to_inf[cluster_name]
        #             to_write["storage_size"] = 0
        #             to_write["databases"] = {}
        #
        #             for shard, process_info in shards.items():
        #                 print(f"shardpid: {process_info['pid']}")
        #
        group_id=cluster.get('groupId')
        process_id=process.get('id')
        resp = self.get_database_for_process(group_id,process_id)
        process["databases_size"] = resp.get("results",[]) #convert_list_to_dict(resp.get("results",[]))


    #def get_database_size_for_process(self,cluster,process,databaseName):
    #    measurement_database_base_url = f"{list_databases_base_url}/{database['databaseName']}/measurements"
    #    print(f"measurementDatabaseBaseUrl: {measurement_database_base_url}")
    #
    #                     database_measure = await atlas_request("GetProcessDatabasesMeasure", measurement_database_base_url, "2023-01-01", {
    #                         'm': "DATABASE_STORAGE_SIZE",
    #                         'granularity': "PT10M",
    #                         'period': "PT1H"
    #                     })

    def get_database_composition_sizing_for_process(self,cluster,process):
        databases_size = process.get("databases_size",{})
       # for database,size in databases_size.items():
       #     database
       #     print(f"databaseMeasure: {database_measure}")
    #
    #                     measurement = database_measure['measurements'][0]
    #                     measurement['dataPoints'] = [dp for dp in measurement['dataPoints'] if dp['value'] is not None]
    #                     if measurement['dataPoints']:
    #                         if database['databaseName'] not in to_write["databases"]:
    #                             to_write["databases"][database['databaseName']] = 0
    #                         value = measurement['dataPoints'][-1]['value']
    #                         to_write["databases"][database['databaseName']] += value
    #                         to_write["storage_size"] += value
    #
    #             if to_write["databases"]:
    #                 databases = to_write["databases"]
    #                 del to_write["databases"]
    #                 for dbs, size in databases.items():
    #                     to_write["database"] = dbs
    #                     to_write["databaseSize"] = size
    #                     to_write["databasePerc"] = (size * 100) / to_write["storage_size"]
    #                     print(f"toWrite: {to_write}")
    #                     collection.insert_one(to_write)
    #                     count += 1
    #
    # except Exception as error:
    #     print("Error occurred: ", error)
    #
    # return count






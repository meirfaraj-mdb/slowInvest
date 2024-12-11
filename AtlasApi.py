import requests
from requests.auth import HTTPDigestAuth
import json
from slowQuery import *
import re

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
    def retrieveLast24HSlowQueriesFromCluster(self,groupId,processId, output_file_path):
        path=f"/groups/{groupId}/processes/{processId}/performanceAdvisor/slowQueryLogs"
        resp=self.atlas_request('SlowQueries', path, '2023-01-01', {})
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

    def listAllProject(self):
        resp=self.atlas_request("GetAllProject",
                              "/groups",
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




    def calculate_instance_composition(self, cluster):
        instance_composition = {}
        if cluster['clusterType'] == "SHARDED":
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
    def get_clusters_composition(self,group_id=None,cluster_name=None):
        result=[]
        #Group_id None not yet supported
        #project_ids = []
        #    for project in projects_response['results']:
        #        project_ids.append(project['id'])
        #        project_id_to_name[project['id']] = project['name']
        #    for project_id in project_ids:

        processes = self.getAllProcessesForProject(group_id)
        processes = processes.get('results',[])

        if cluster_name is None:
            clusters = self.listAllProjectClusters(group_id)
        else:
            clusters = [ self.getOneCluster(group_id,cluster_name)]

        cluster_name_to_inf = {}
        processes_by_cluster = {}
        for cluster in clusters.get('results',[]):
            cluster_name=cluster.get('name')
            cluster_name_to_inf[cluster_name.lower()] = cluster
            if cluster.get('paused',False):
                print(f"{cluster_name} is paused skip instance composition and processes!")
                result.append(cluster)
                continue
            cluster=self.calculate_instance_composition(cluster)
            cluster["advancedConfiguration"]=self.getAdvancedConfigurationForOneCluster(group_id, cluster_name)
            result.append(cluster)
        for process in processes:
            user_alias = process['userAlias']
            pattern_config = r"(.+)-(config)-(\d+)-(\d+)"
            match_config = re.match(pattern_config, user_alias)
            if match_config:
                cluster_name = match_config.group(1)
                shard_number = int(match_config.group(3))
                configServerType = cluster_name_to_inf.get(cluster_name.lower(),{}).get('configServerType',None)
                if cluster_name not in processes_by_cluster:
                    processes_by_cluster[cluster_name] = {}
                processes_by_cluster[cluster_name]["config"] = process
                if configServerType == 'EMBEDDED':
                    replicationSpecs = cluster_name_to_inf.get(cluster_name.lower(),{}).get('replicationSpecs',[])
                    shard_number = len(replicationSpecs)-1
                    print(f"{shard_number}")
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












    #         for cluster_name, shards in processes_by_cluster.items():
    #             to_write = cluster_name_to_inf[cluster_name]
    #             to_write["storage_size"] = 0
    #             to_write["databases"] = {}
    #
    #             for shard, process_info in shards.items():
    #                 print(f"shardpid: {process_info['pid']}")
    #
    #                 process_base_url = f"{project_base_url}/processes/{process_info['pid']}"
    #                 list_databases_base_url = f"{process_base_url}/databases"
    #
    #                 databases = await atlas_request("GetProcessDatabases", list_databases_base_url, "2023-01-01", {
    #                     'itemsPerPage': '500',
    #                     'pageNum': '1',
    #                 })
    #                 print(f"databases.totalCount: {databases['totalCount']}")
    #
    #                 for database in databases['results']:
    #                     measurement_database_base_url = f"{list_databases_base_url}/{database['databaseName']}/measurements"
    #                     print(f"measurementDatabaseBaseUrl: {measurement_database_base_url}")
    #
    #                     database_measure = await atlas_request("GetProcessDatabasesMeasure", measurement_database_base_url, "2023-01-01", {
    #                         'm': "DATABASE_STORAGE_SIZE",
    #                         'granularity': "PT10M",
    #                         'period': "PT1H"
    #                     })
    #                     print(f"databaseMeasure: {database_measure}")
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






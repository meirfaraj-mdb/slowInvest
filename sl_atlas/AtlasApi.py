import concurrent
import logging
import re
import time
from datetime import datetime
import csv
import io
import msgspec
import requests
from requests.auth import HTTPDigestAuth

from dateutil.relativedelta import relativedelta
import sl_utils.utils
from sl_async.gzip import BufferedGzipWriter
from sl_async.slatlas import BufferedSlAtlasSource
from sl_async.slorch import AsyncExtractAndAggregate
from sl_utils.utils import remove_extension, createDirs
from datetime import datetime, timedelta
from collections import defaultdict

atlas_logging = logging.getLogger("atlas")
atlas_logging.setLevel(logging.DEBUG)

decoder = msgspec.json.Decoder()
encoder = msgspec.json.Encoder()

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

    def atlas_request(self, op, fpath, fdate, arg=None, req_type="GET", body=None):
        apiBaseURL = '/api/atlas/v2'
        url = f"https://cloud.mongodb.com{apiBaseURL}{fpath}"
        headers = {
            'Accept': f"application/vnd.atlas.{fdate}+json",
            'Content-Type': f"application/vnd.atlas.{fdate}+json"
        }
        try:
            if req_type.upper() == "GET":
                response = requests.get(
                    url,
                    params=arg,
                    auth=HTTPDigestAuth(self.PUBLIC_KEY, self.PRIVATE_KEY),
                    headers=headers
                )
            elif req_type.upper() == "POST":
                response = requests.post(
                    url,
                    params=arg,
                    json=body,  # FIX: Send JSON properly
                    auth=HTTPDigestAuth(self.PUBLIC_KEY, self.PRIVATE_KEY),
                    headers=headers
                )
            else:
                raise ValueError(f"Unsupported request type: {req_type}")

                # Check if the response was successful
            if response.status_code not in (200, 201):
                print(f"Error in {op}: {response.status_code} - {response.text}")
                response.raise_for_status()  # Raise for non-OK responses
            if op == "Create Cost Explorer Process" or op =="Check Cost Explorer Process Status":
                print(f"{op} : resp : {response.text}")
            return decoder.decode(response.text)

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {op}: {e}")
            raise


    def atlas_request_csv(self, op, fpath, fdate, arg):
        """
        Function to request Atlas Admin API returning CSV data.
        """
        apiBaseURL = '/api/atlas/v2'
        url = f"https://cloud.mongodb.com{apiBaseURL}{fpath}"
        headers = {
            'Accept': f"application/vnd.atlas.{fdate}+csv",
            'Content-Type': f"application/vnd.atlas.{fdate}+csv"
        }
        try:
            response = requests.get(
                url,
                params=arg,
                auth=HTTPDigestAuth(self.PUBLIC_KEY, self.PRIVATE_KEY),
                headers=headers
            )
            if response.status_code != 200:
                print(f"Error in {op}: {response.status_code} - {response.text}")
                response.raise_for_status()

                # Parse CSV into list of dicts
            csv_text = response.text
            reader = csv.DictReader(io.StringIO(csv_text))
            return list(reader)

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {op}: {e}")
            raise

    # retrieve slow queries
    def retrieveLast24HSlowQueriesFromCluster(self,groupId,processId,shard, output_file_path, chunk_size=50000,save_by_chunk="none"):
        output_file_path_without_ext = remove_extension(output_file_path)
        parquet_file_path_base=f"{output_file_path_without_ext}/"
        createDirs(parquet_file_path_base)

        src= BufferedSlAtlasSource(self,groupId,processId)
        sl_output_file_path = f"{output_file_path}/slow_queries_{groupId}_{processId}.log"

        dest= BufferedGzipWriter(sl_output_file_path)
        orch=AsyncExtractAndAggregate(processId,shard,src,dest,parquet_file_path_base,chunk_size,save_by_chunk  )
        src.set_dtime(orch.get_dtime())
        orch.run()
        return orch.get_results()


    def get_cluster_billing_sku_evolution(self, org_id, cluster_name=None):
        """
        Retrieve last 3 months excluding current month, grouped by month and SKU with evolution %.
        Always uses CSV via two-step Cost Explorer process.
        """
        #TOdo :search for orgID in /api/atlas/v2/clusters
        if org_id is None :
            print(f"Org not provided no yet orgId disco")
            return None

        # ---- Step 1: date calculations ----
        today = datetime.utcnow()
        first_day_current_month = datetime(today.year, today.month, 1)
        end_last_month = first_day_current_month - timedelta(days=1)
        start_range = end_last_month - relativedelta(months=2)
        start_range = datetime(start_range.year, start_range.month, 1)

        # ---- Step 2: Create cost explorer process ----
        fdate = "2025-03-12"
        fpath_create = f"/orgs/{org_id}/billing/costExplorer/usage"

        body = {
            "startDate": start_range.strftime("%Y-%m-%d"),
            "endDate": end_last_month.strftime("%Y-%m-%d"),
            "granularity": "MONTH",
            "groupBy": ["services"],
        }
        if cluster_name:
            body["clusterName"] = cluster_name

        create_resp = self.atlas_request(
            "Create Cost Explorer Process",  fpath_create, fdate,req_type="POST", body=body
        )

        token = create_resp.get("token")
        if not token:
            raise RuntimeError("No token returned from Cost Explorer create request.")

            # ---- Step 3: Poll until ready ----
        ready = False
        fpath_status = f"/orgs/{org_id}/billing/costExplorer/usage/{token}"
        for _ in range(30):  # max ~60 seconds
            status_resp = self.atlas_request(
                "Check Cost Explorer Process Status", fpath_status, fdate
            )
            if status_resp.get("status") == "COMPLETED":
                ready = True
                break
            elif status_resp.get("status") == "FAILED":
                raise RuntimeError("Cost Explorer process failed.")
            time.sleep(2)

        if not ready:
            raise TimeoutError("Cost Explorer process not ready after polling.")

            # ---- Step 4: Download CSV ----
        fpath_csv = f"/orgs/{org_id}/billing/costExplorer/usage/{token}/csv"
        csv_data = self.atlas_request_csv(
            "Download Cost Explorer CSV", fpath_csv, fdate
        )

        # ---- Step 5: Transform into monthly SKU evolution output ----
        month_data = defaultdict(lambda: {"totalcost": 0, "sku": {}})
        for row in csv_data:
            period = row["date"][:7].replace("-", "/")
            sku = row["sku"]
            cost = float(row.get("cost", 0) or 0)
            month_data[period]["totalcost"] += cost
            month_data[period]["sku"][sku] = {"cost": cost}

        months_sorted = sorted(month_data.keys())
        if not months_sorted:
            return {}

        range_start_month = months_sorted[0]
        for i, month in enumerate(months_sorted):
            totalcost = month_data[month]["totalcost"]
            for sku in month_data[month]["sku"]:
                cost_val = month_data[month]["sku"][sku]["cost"]

                # Percent of monthly total
                month_data[month]["sku"][sku]["percent_monthly_cost"] = (
                    round(cost_val / totalcost * 100, 2) if totalcost else 0
                )

                # Evolution from previous month
                if i > 0:
                    prev_sku_cost = month_data[months_sorted[i-1]]["sku"].get(sku, {}).get("cost", 0)
                    month_data[month]["sku"][sku]["evolution_previous_month_in_perc"] = (
                        round((cost_val - prev_sku_cost) / prev_sku_cost * 100, 2) if prev_sku_cost else None
                    )

                    # Evolution from start
                start_sku_cost = month_data[range_start_month]["sku"].get(sku, {}).get("cost", 0)
                month_data[month]["sku"][sku]["evolution_from_range_start_in_perc"] = (
                    round((cost_val - start_sku_cost) / start_sku_cost * 100, 2) if start_sku_cost else None
                )

                # Month total evolutions
            if i > 0:
                prev_total = month_data[months_sorted[i-1]]["totalcost"]
                month_data[month]["evolution_previous_month_in_perc"] = (
                    round((totalcost - prev_total) / prev_total * 100, 2) if prev_total else None
                )
            start_total = month_data[range_start_month]["totalcost"]
            month_data[month]["evolution_from_range_start_in_perc"] = (
                round((totalcost - start_total) / start_total * 100, 2) if start_total else None
            )

        return dict(month_data)

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


    #https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}
    #    def
    def getAutoScalingEvent(self,group_id,cluster_names=None, start_date=None, num_months=None):
        """
            Retrieve auto-scaling events for the given project and optional cluster(s).

            Parameters:
                group_id (str): MongoDB Atlas Project ID.
                cluster_names (list[str], optional): Filter events by these cluster names.
                start_date (str, optional): Explicit start date string (YYYY-MM-DD).
                num_months (int, optional): If set, calculates minDate = now - num_months months in UTC.
        """
        total=0
        page_num=1
        args = {
            "itemsPerPage":"500",
            "pageNum":str(page_num),
            "includeRaw":"true",
            "eventType":[
                "COMPUTE_AUTO_SCALE_INITIATED",
                "DISK_AUTO_SCALE_INITIATED",
                "COMPUTE_AUTO_SCALE_INITIATED_BASE",
                "COMPUTE_AUTO_SCALE_INITIATED_ANALYTICS",
                "COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_BASE",
                "COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_ANALYTICS",
                "COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_BASE",
                "COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_ANALYTICS",
                "DISK_AUTO_SCALE_MAX_DISK_SIZE_FAIL",
                "COMPUTE_AUTO_SCALE_OPLOG_FAIL_BASE",
                "COMPUTE_AUTO_SCALE_OPLOG_FAIL_ANALYTICS",
                "DISK_AUTO_SCALE_OPLOG_FAIL"]
        }
        if not(cluster_names is None):
            args["clusterNames"]=cluster_names
        # Handle date parameters
        if num_months is not None:
            # Calculate now - num_months in UTC
            min_date = (datetime.utcnow() - relativedelta(months=num_months)).strftime("%Y-%m-%dT%H:%M:%SZ")
            args["minDate"] = min_date
        elif start_date is not None:
            # Pass explicit start date (assuming it's already in correct format)
            args["minDate"] = start_date

        resp=None
        results=[]
        while (resp is None) or total<len(results):
            resp=self.atlas_request("getAutoScalingEvent",
                                f"/groups/{group_id}/events",
                                "2023-01-01",
                                args)
            total=resp.get("totalCount",0)
            results+=resp.get("results",[])
        scalingByCluster = {}
        for result in results:
            cluster_name=result.get("clusterName","None")
            cur=scalingByCluster.get(cluster_name,[])
            cur.append(result)
            scalingByCluster[cluster_name]=cur
        return scalingByCluster
#('id','created', 'clusterName','computeAutoScalingTriggers','eventTypeName','raw.computeAutoScaleTriggers','raw.isAtMaxCapacityAfterAutoScale')
#{'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events?includeRaw=true&eventType=COMPUTE_AUTO_SCALE_INITIATED&eventType=DISK_AUTO_SCALE_INITIATED&eventType=COMPUTE_AUTO_SCALE_INITIATED_BASE&eventType=COMPUTE_AUTO_SCALE_INITIATED_ANALYTICS&eventType=COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_BASE&eventType=COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_ANALYTICS&eventType=COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_BASE&eventType=COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_ANALYTICS&eventType=DISK_AUTO_SCALE_MAX_DISK_SIZE_FAIL&eventType=COMPUTE_AUTO_SCALE_OPLOG_FAIL_BASE&eventType=COMPUTE_AUTO_SCALE_OPLOG_FAIL_ANALYTICS&eventType=DISK_AUTO_SCALE_OPLOG_FAIL&pageNum=1&itemsPerPage=500', 'rel': 'self'}],
    # 'results': [{
    # : 'Cluster0',
    # :
    # 'CPU_ABOVE',
    # ': '2025-01-20T09:02:26Z',
    # : 'COMPUTE_AUTO_SCALE_INITIATED_BASE',
    # 'groupId': '663bef88730dee3831c03033',
    # : '678e11221ad43c4f968d8f5c',
    # 'isGlobalAdmin': False,
    # 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/678e11221ad43c4f968d8f5c', 'rel': 'self'}],
    # 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT',
    #         'accountUserId': None,
    #         'alertConfigId': None,
    #         'alertHHMMTT': '4:02 AM EST', 'alertYYYYMMDD': '2025/01/20',
    #         'analyticsBoundsUpdates': None,
    #         'baseBoundsUpdates': None,
    #         'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False},
    #         'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '678e0263b5966b0ad6b8f870',
    #         'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier',
    #         'computeAutoScaleSkipped': None,
    #         'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...},
    #         'type': 'CPU_ABOVE', 'windowSeconds': 1200}], 'cre': '2025-01-20T09:02:26Z', 'currentInstanceSize': None,
    #         'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE',
    #         'gn': None, 'hidden': False, 'id': '678e11221ad43c4f968d8f5c', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False,
    #         'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True,
    #         'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False,
    #         'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None,
    #         'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None,
    #         'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.08',
    #         'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': 'atlas-9wucy2-config-0',
    #         'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0,
    #         'resourceIds': [{'id': '678e0263b5966b0ad6b8f870', 'type': 'CLUSTER'}],
    #         'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None},
    #         'memory': {'threshold': None, 'triggered': False, 'windowString': None}},
    #         'scaleUp': {'cpu': {'threshold': 90, 'throttled': True, 'triggered': True, 'windowString': '20 minutes'},
    #         'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None},
    #         'memory': {'threshold': None, 'triggered': False, 'windowString': None}},
    #         'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}},
    #         {'clusterName': 'Cluster0', 'created': '2025-01-08T14:35:59Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED',
    #         'groupId': '663bef88730dee3831c03033', 'id': '677e8d4fb2c3f56f2438f3be', 'isGlobalAdmin': False,
    #         'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/677e8d4fb2c3f56f2438f3be',
    #         'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '9:35 AM EST', 'alertYYYYMMDD': '2025/01/08',
    #         'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '677e5f65362ca57de04562dd', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2025-01-08T14:35:59Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '677e8d4fb2c3f56f2438f3be', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 19.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 14.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '677e5f65362ca57de04562dd', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2025-01-08T14:04:14Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '677e85ded1e0be7a2104c8f2', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/677e85ded1e0be7a2104c8f2', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '9:04 AM EST', 'alertYYYYMMDD': '2025/01/08', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '677e5f65362ca57de04562dd', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2025-01-08T14:04:14Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '677e85ded1e0be7a2104c8f2', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 14.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.08', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '677e5f65362ca57de04562dd', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster1', 'created': '2024-12-17T10:28:03Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67615233d2b8bc636118b6b0', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67615233d2b8bc636118b6b0', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '5:28 AM EST', 'alertYYYYMMDD': '2024/12/17', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '675725ef76d34a12e9267cde', 'clusterName': 'Cluster1', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-17T10:28:03Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67615233d2b8bc636118b6b0', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.27', 'newDiskSizeGB': 13.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.25', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '675725ef76d34a12e9267cde', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'CPU_ABOVE', 'created': '2024-12-11T07:11:48Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '67593b34f788672310841a2a', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67593b34f788672310841a2a', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '2:11 AM EST', 'alertYYYYMMDD': '2024/12/11', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'CPU_ABOVE', 'windowSeconds': 3600}], 'cre': '2024-12-11T07:11:48Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '67593b34f788672310841a2a', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'scaleUp': {'cpu': {'threshold': 75, 'triggered': True, 'windowString': '1 hours'}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'SCALE_INTERVAL_ABOVE, CPU_BELOW, MEMORY_WITH_WT_USAGE_BELOW, WT_USAGE_BELOW', 'created': '2024-12-10T23:00:52Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '6758c824e544b967cb5a4f23', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/6758c824e544b967cb5a4f23', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '6:00 PM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'SCALE_INTERVAL_ABOVE', 'windowSeconds': 86400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'CPU_BELOW', 'windowSeconds': 14400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'MEMORY_WITH_WT_USAGE_BELOW', 'windowSeconds': 14400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'WT_USAGE_BELOW', 'windowSeconds': 14400}], 'cre': '2024-12-10T23:00:52Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '6758c824e544b967cb5a4f23', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.09', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M10', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.21', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M20', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': 50, 'triggered': True, 'windowString': '4 hours'}, 'memory': {'threshold': 60, 'triggered': True, 'windowString': '4 hours', 'wt': True}}, 'scaleUp': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'CPU_ABOVE', 'created': '2024-12-10T18:35:03Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '675889d73465b411c80335ce', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/675889d73465b411c80335ce', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '1:35 PM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {...}, 'type': 'CPU_ABOVE', 'windowSeconds': 3600}], 'cre': '2024-12-10T18:35:03Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '675889d73465b411c80335ce', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'scaleUp': {'cpu': {'threshold': 75, 'triggered': True, 'windowString': '1 hours'}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2024-12-10T13:14:13Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67583ea54852222cc9268a70', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67583ea54852222cc9268a70', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '8:14 AM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-10T13:14:13Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67583ea54852222cc9268a70', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 19.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 14.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2024-12-10T13:04:14Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67583c4e0166d6776a123b74', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67583c4e0166d6776a123b74', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '8:04 AM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-10T13:04:14Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67583c4e0166d6776a123b74', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 14.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.08', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}],
    #         'totalCount': 9}

#[{'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'CPU_ABOVE', 'created': '2025-01-20T09:02:26Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '678e11221ad43c4f968d8f5c', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/678e11221ad43c4f968d8f5c', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '4:02 AM EST', 'alertYYYYMMDD': '2025/01/20', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '678e0263b5966b0ad6b8f870', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'NORMALIZED_AUTO_SCALE_SYSTEM_CPU', 'mode': 'AVERAGE', 'op': 'GREATER_THAN', 'threshold': 0.8999999761581421, 'units': 'RAW'}, 'type': 'CPU_ABOVE', 'windowSeconds': 1200}], 'cre': '2025-01-20T09:02:26Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '678e11221ad43c4f968d8f5c', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.08', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': 'atlas-9wucy2-config-0', 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '678e0263b5966b0ad6b8f870', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'scaleUp': {'cpu': {'threshold': 90, 'throttled': True, 'triggered': True, 'windowString': '20 minutes'}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2025-01-08T14:35:59Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '677e8d4fb2c3f56f2438f3be', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/677e8d4fb2c3f56f2438f3be', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '9:35 AM EST', 'alertYYYYMMDD': '2025/01/08', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '677e5f65362ca57de04562dd', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2025-01-08T14:35:59Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '677e8d4fb2c3f56f2438f3be', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 19.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 14.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '677e5f65362ca57de04562dd', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2025-01-08T14:04:14Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '677e85ded1e0be7a2104c8f2', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/677e85ded1e0be7a2104c8f2', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '9:04 AM EST', 'alertYYYYMMDD': '2025/01/08', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '677e5f65362ca57de04562dd', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2025-01-08T14:04:14Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '677e85ded1e0be7a2104c8f2', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 14.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.08', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '677e5f65362ca57de04562dd', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster1', 'created': '2024-12-17T10:28:03Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67615233d2b8bc636118b6b0', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67615233d2b8bc636118b6b0', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '5:28 AM EST', 'alertYYYYMMDD': '2024/12/17', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': None, 'newMinInstanceSize': None, 'previousMaxInstanceSize': None, 'previousMinInstanceSize': None, 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '675725ef76d34a12e9267cde', 'clusterName': 'Cluster1', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-17T10:28:03Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67615233d2b8bc636118b6b0', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.27', 'newDiskSizeGB': 13.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.25', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '675725ef76d34a12e9267cde', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'CPU_ABOVE', 'created': '2024-12-11T07:11:48Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '67593b34f788672310841a2a', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67593b34f788672310841a2a', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '2:11 AM EST', 'alertYYYYMMDD': '2024/12/11', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'NORMALIZED_AUTO_SCALE_SYSTEM_CPU', 'mode': 'AVERAGE', 'op': 'GREATER_THAN', 'threshold': 0.75, 'units': 'RAW'}, 'type': 'CPU_ABOVE', 'windowSeconds': 3600}], 'cre': '2024-12-11T07:11:48Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '67593b34f788672310841a2a', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'scaleUp': {'cpu': {'threshold': 75, 'triggered': True, 'windowString': '1 hours'}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'SCALE_INTERVAL_ABOVE, CPU_BELOW, MEMORY_WITH_WT_USAGE_BELOW, WT_USAGE_BELOW', 'created': '2024-12-10T23:00:52Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '6758c824e544b967cb5a4f23', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/6758c824e544b967cb5a4f23', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '6:00 PM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'AUTO_SCALE_INTERVAL', 'mode': 'TOTAL', 'op': 'GREATER_THAN', 'threshold': 86400.0, 'units': 'SECONDS'}, 'type': 'SCALE_INTERVAL_ABOVE', 'windowSeconds': 86400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'NORMALIZED_AUTO_SCALE_SYSTEM_CPU', 'mode': 'AVERAGE', 'op': 'LESS_THAN', 'threshold': 0.5, 'units': 'RAW'}, 'type': 'CPU_BELOW', 'windowSeconds': 14400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'AUTO_SCALE_SYSTEM_MEMORY', 'mode': 'AVERAGE', 'op': 'LESS_THAN', 'threshold': 0.6000000238418579, 'units': 'RAW'}, 'type': 'MEMORY_WITH_WT_USAGE_BELOW', 'windowSeconds': 14400}, {'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'AUTO_SCALE_WT_USAGE', 'mode': 'AVERAGE', 'op': 'LESS_THAN', 'threshold': 0.8999999761581421, 'units': 'RAW'}, 'type': 'WT_USAGE_BELOW', 'windowSeconds': 14400}], 'cre': '2024-12-10T23:00:52Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '6758c824e544b967cb5a4f23', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.09', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M10', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.21', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M20', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': 50, 'triggered': True, 'windowString': '4 hours'}, 'memory': {'threshold': 60, 'triggered': True, 'windowString': '4 hours', 'wt': True}}, 'scaleUp': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'computeAutoScalingTriggers': 'CPU_ABOVE', 'created': '2024-12-10T18:35:03Z', 'eventTypeName': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'groupId': '663bef88730dee3831c03033', 'id': '675889d73465b411c80335ce', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/675889d73465b411c80335ce', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '1:35 PM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': None, 'boundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': 'base cluster tier', 'clusterTierLabelPlural': 'base cluster's tier', 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [{'calculatedMetrics': 0.0, 'internalMetadata': None, 'threshold': {'metric': 'NORMALIZED_AUTO_SCALE_SYSTEM_CPU', 'mode': 'AVERAGE', 'op': 'GREATER_THAN', 'threshold': 0.75, 'units': 'RAW'}, 'type': 'CPU_ABOVE', 'windowSeconds': 3600}], 'cre': '2024-12-10T18:35:03Z', 'currentInstanceSize': None, 'description': 'Compute auto-scaling initiated for base tier', 'et': 'COMPUTE_AUTO_SCALE_INITIATED_BASE', 'gn': None, 'hidden': False, 'id': '675889d73465b411c80335ce', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': True, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': None, 'newBaseInstanceSize': None, 'newCostPerHour': '0.21', 'newDiskSizeGB': 0.0, 'newInstanceSize': 'M20', 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': None, 'originalBaseInstanceSize': None, 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 0.0, 'originalInstanceSize': 'M10', 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': {'cpu': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'scaleUp': {'cpu': {'threshold': 75, 'triggered': True, 'windowString': '1 hours'}, 'lowInstanceSize': {'threshold': None, 'triggered': False, 'windowString': None}, 'memory': {'threshold': None, 'triggered': False, 'windowString': None}}, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2024-12-10T13:14:13Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67583ea54852222cc9268a70', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67583ea54852222cc9268a70', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '8:14 AM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-10T13:14:13Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67583ea54852222cc9268a70', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 19.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.09', 'originalDiskSizeGB': 14.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}, {'clusterName': 'Cluster0', 'created': '2024-12-10T13:04:14Z', 'eventTypeName': 'DISK_AUTO_SCALE_INITIATED', 'groupId': '663bef88730dee3831c03033', 'id': '67583c4e0166d6776a123b74', 'isGlobalAdmin': False, 'links': [{'href': 'https://cloud.mongodb.com/api/atlas/v2/groups/663bef88730dee3831c03033/events/67583c4e0166d6776a123b74', 'rel': 'self'}], 'raw': {'_t': 'NDS_AUTO_SCALING_AUDIT', 'accountUserId': None, 'alertConfigId': None, 'alertHHMMTT': '8:04 AM EST', 'alertYYYYMMDD': '2024/12/10', 'analyticsBoundsUpdates': None, 'baseBoundsUpdates': {'maxInstanceSizeUpdated': False, 'minInstanceSizeUpdated': False, 'newMaxInstanceSize': 'M20', 'newMinInstanceSize': 'M10', 'previousMaxInstanceSize': 'M20', 'previousMinInstanceSize': 'M10', 'scaleDownDisabled': False}, 'boundsUpdates': None, 'canIncreaseMaxInstanceSize': False, 'cid': '663bef88730dee3831c03033', 'clusterId': '6734b10e8e32b9054c5d957d', 'clusterName': 'Cluster0', 'clusterTierLabel': None, 'clusterTierLabelPlural': None, 'computeAutoScaleSkipped': None, 'computeAutoScaleTriggers': [], 'cre': '2024-12-10T13:04:14Z', 'currentInstanceSize': None, 'description': 'Disk auto-scaling initiated', 'et': 'DISK_AUTO_SCALE_INITIATED', 'gn': None, 'hidden': False, 'id': '67583c4e0166d6776a123b74', 'isAlert': True, 'isAnalyticsInstanceSizeScaled': False, 'isAnyTierScaled': False, 'isAtMaxCapacityAfterAutoScale': False, 'isBaseInstanceSizeScaled': False, 'isMmsAdmin': False, 'isNearMaxCapacityAfterAutoScale': False, 'maxDiskSize': 0.0, 'newAnalyticsInstanceSize': 'M10', 'newBaseInstanceSize': 'M10', 'newCostPerHour': '0.09', 'newDiskSizeGB': 14.0, 'newInstanceSize': None, 'orgId': None, 'orgName': None, 'originalAnalyticsInstanceSize': 'M10', 'originalBaseInstanceSize': 'M10', 'originalCostPerHour': '0.08', 'originalDiskSizeGB': 10.0, 'originalInstanceSize': None, 'remoteAddr': None, 'replicaSetId': None, 'replicationAvailableOplog': 0, 'replicationRequiredOplog': 0, 'resourceIds': [{'id': '6734b10e8e32b9054c5d957d', 'type': 'CLUSTER'}], 'scaleDown': None, 'scaleUp': None, 'severity': 'WARNING', 'source': 'SYSTEM', 'un': None, 'userApiKeyType': None, 'userId': None, 'ut': None}}]


#https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/processes/{processId}/measurements
#Returns disk, partition, or host measurements per process for the specified host for the specified project.
# Returned value can be one of the following:
# Throughput of I/O operations for the disk partition used for the MongoDB process
# Percentage of time during which requests the partition issued and serviced
# Latency per operation type of the disk partition used for the MongoDB process
# Amount of free and used disk space on the disk partition used for the MongoDB process
# Measurements for the host, such as CPU usage or number of I/O operations
# To use this resource, the requesting API Key must have the Project Read Only role.
    def getAllMeasurementforProcess(self,group_id,process_id):
            resp = self.atlas_request(
                "get_database_for_process",
                f"/groups/{group_id}/processes/{process_id}/measurements",
                "2023-01-01",
                {
                    'period': 'PT24H',
                    'granularity': 'PT1M',
                }
            )
            return resp

    def getAllDiskforProcess(self,group_id,process_id):
        resp = self.atlas_request(
            "get_disks_for_process",
            f"/groups/{group_id}/processes/{process_id}/disks",
            "2025-03-12"
        )
        return resp

    def getDiskMeasurementforProcess(self,group_id,process_id,partitionName):
        resp = self.atlas_request(
            "get_database_for_process",
            f"/groups/{group_id}/processes/{process_id}/disks/{partitionName}/measurements",
            "2023-01-01",
            {
                'period': 'PT24H',
                'granularity': 'PT1M',
            }
        )
        return resp


    def getAllDiskMetricsforProcess(self,group_id,process_id):
        disks = self.getAllDiskforProcess(group_id,process_id)
        disks = disks.get("results",[])
        ##{'...'results': [{'links': [ 'partitionName': 'data'}], 'totalCount': 1}
       # for disk in disks:
       #     c

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
        if cluster['clusterType'] == "SHARDED" and (cluster["configServerType"]!="EMBEDDED") :
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
    def get_clusters_composition(self,group_id=None,cluster_name=None,full=True,scaling_start_date=None,scaling_num_month=None,orgId=None):
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
            cluster["loadedFromApi"]=True
            cluster["freshness"]=time.time()
            cluster["future"]={}
            if full:
                cluster["future"]["performanceAdvisorSuggestedIndexes"] = pool.submit(self.getPerformanceAdvisorSuggestedIndexes,group_id,cluster_name)
                cluster["future"]["onlineArchiveForOneCluster"] = pool.submit(self.getAllOnlineArchiveForOneCluster,group_id,cluster_name)
                cluster["future"]["backupCompliance"] = pool.submit(self.getBackupCompliance,group_id)
                cluster["future"]["backup_snapshot"] = pool.submit(self.listAllBackupSnapshotForCluster,group_id,cluster_name,cluster["clusterType"])
                cluster["future"]["advancedConfiguration"] = pool.submit(self.getAdvancedConfigurationForOneCluster,group_id, cluster_name)
                cluster["future"]["scaling"] = pool.submit(self.getAutoScalingEvent,group_id,[cluster_name],scaling_start_date,scaling_num_month)
                cluster["future"]["billing"] = pool.submit(self.get_cluster_billing_sku_evolution, orgId, cluster_name)


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
            if full:
                process["future"]=process.get("future",{})
                process["future"]["measurement"] = pool.submit(self.getAllMeasurementforProcess,group_id, process.get("id",None))
                process["future"]["disk_measurement"] = pool.submit(self.getAllDiskMetricsforProcess,group_id, process.get("id",None))



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

    def extract_failure_cause(self,event_type):
        switch_case = {
            "COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_BASE": "SCALE_DOWN",
            "COMPUTE_AUTO_SCALE_SCALE_DOWN_FAIL_ANALYTICS": "SCALE_DOWN_A",
            "COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_BASE": "MAX_INSTANCE_SIZE",
            "COMPUTE_AUTO_SCALE_MAX_INSTANCE_SIZE_FAIL_ANALYTICS": "MAX_INSTANCE_SIZE_A",
            "DISK_AUTO_SCALE_MAX_DISK_SIZE_FAIL": "MAX_DISK_SIZE_FAIL",
            "COMPUTE_AUTO_SCALE_OPLOG_FAIL_BASE": "OPLOG_FAIL_BASE",
            "COMPUTE_AUTO_SCALE_OPLOG_FAIL_ANALYTICS": "OPLOG_FAIL_A",
            "DISK_AUTO_SCALE_OPLOG_FAIL": "OPLOG_FAIL"
        }
        # Return the matching failure cause or an empty string if not found
        return switch_case.get(event_type, '')
    def update_scaling_alert(self,cluster):
        scaling=cluster.get("scaling",[])
        for scal in scaling:
            eventTypeName=scal.get('eventTypeName',"")
            scal["scal_type"]="COMPUTE" if eventTypeName.startswith("COMPUTE") else "DISK"
            scal["scal_succeed"]="_FAIL" not in eventTypeName
            scal["scal_fail_cause"]=self.extract_failure_cause(eventTypeName)
            if not scal["scal_succeed"]:
                custom_alert=cluster["custom_alert"]=cluster.get("custom_alert",{})
                scaling_alert=custom_alert["scaling"]=custom_alert.get("scaling",{})
                cause=scal["scal_fail_cause"]
                cur=scaling_alert[cause] = scaling_alert.get(cause,{})
                cur["count"]=cur.get("count",0)+1
                cur["times"]=cur.get("times",[])
                cur["times"].append(scal.get("created",""))
            computeAutoScaleTriggers=scal.get('raw',{}).get("computeAutoScaleTriggers",[])
            if computeAutoScaleTriggers is None or len(computeAutoScaleTriggers)==0:
                computeAutoScaleTriggers=""
            scal["compute_auto_scaling_triggers"]=scal.get('computeAutoScalingTriggers',"")+"\n"+str(computeAutoScaleTriggers)

    def update_one_future(self,cluster,name):
        cluster_name = cluster.get("name","")
        atlas_logging.debug(f"cluster {cluster_name} updating future {name}")
        future = cluster.get("future",{}).get(name,None)
        if future is None:
            atlas_logging.debug(f"future {name} not found")
            return
        concurrent.futures.as_completed(future)
        try:
            cluster[name]=future.result()
            if name in ["scaling"]:
                cluster[name]=cluster[name].get(cluster_name,[])
                self.update_scaling_alert(cluster)
        except Exception as exc:
            atlas_logging.debug(f"failed to retrieve the {name} : {exc}")
            cluster[f"{name}_configured"]="Fail to retrieve"
            cluster[f"{name}_count"]=0
        else:
            elem=cluster.get(name, None);
            cluster[f"{name}_count"]=len(elem) if not (elem is None) and len(elem) > 0 else 0
            cluster[f"{name}_configured"]="True" if cluster.get(f"{name}_count",0) > 0 else "False"
        del cluster["future"][name]


    def update_cluster_future_result(self,cluster):
        keys=list(cluster.get("future",{}).keys())
        name = cluster.get("name","")
        atlas_logging.debug(f"Getting results for cluster {name} for futures {keys}")
        for key in keys:
            self.update_one_future(cluster,key)
        if "future" in cluster:
            del cluster["future"]


    def update_all_cluster_process_future_result(self,cluster):
        name = cluster.get("name","")
        atlas_logging.debug(f"Cluster {name} updating processes futures")



    def save_cluster_result(self,cluster):
        self.update_cluster_future_result(cluster)
        name = cluster.get("name","")
        if cluster["loadedFromApi"]:
            atlas_logging.debug(f"Cluster {name} data where loaded from API need to save them")
            clusterCopy = {}
            exclude=["future","processes"]
            for key in cluster:
                if key in exclude:
                    atlas_logging.debug(f"Cluster {name} will NOT save {key}")
                    continue
                atlas_logging.debug(f"Cluster {name} will save {key}")
                clusterCopy[key] = cluster[key]
            # current date
            name=cluster.get("name")
            current_date = datetime.now()
            # Format the date as a string in the format YYYYMMDD
            date_str = current_date.strftime("%Y%m%d")
            baseClusterPath=f"{self.config.OUTPUT_FILE_PATH}/{name}/conf/"
            baseClusterWDatePath=f"{baseClusterPath}/{date_str}/"
            baseClusterWDateFilePath=f"{baseClusterWDatePath}/cluster.json"
            createDirs(baseClusterWDatePath)

            with open(baseClusterWDateFilePath, 'wb') as clusterFile:
                clusterFile.write(encoder.encode(clusterCopy))

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






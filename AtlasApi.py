import requests
from requests.auth import HTTPDigestAuth
import json
from slowQuery import *


class AtlasApi():
    def __init__(self,config):
        self.config=config
        self.PUBLIC_KEY = self.config.PUBLIC_KEY
        self.PRIVATE_KEY= self.config.PRIVATE_KEY


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

    def listAllProjectClusters(self,groupId):
        resp=self.atlas_request("GetAllCluster",
                                f"/groups/{groupId}/clusters",
                                "2024-08-05",
                                {})
        return resp

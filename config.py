import json
import os
import matplotlib
from matplotlib import colormaps

# Load configuration from a JSON file
def load_config(file_path):
    print(f"loading config from {file_path}")
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def createDirs(directory_path):
    # Create the directory along with any necessary parent directories
    try:
        os.makedirs(directory_path, exist_ok=True)
    except Exception as e:
        print(f"An error occurred: {e}")

class Config():
    def __init__(self, configName):
        # Load the configuration
        if configName:
            self.config = load_config(f"config/{configName}.json")
        else:
            self.config = load_config(f"config/config.json")
        # Atlas / files / OpsManager
        self.RETRIEVAL_MODE = self.config.get('RETRIEVAL_MODE', 'files')
        # Atlas/Ops Manager related
        self.PUBLIC_KEY = self.config.get('PUBLIC_KEY',None)
        self.PRIVATE_KEY = self.config.get('PRIVATE_KEY',None)
        self.GROUP_ID = self.config.get('GROUP_ID',None)
        # one of project, clusters, processId
        self.ATLAS_RETRIEVAL_SCOPE = self.config.get('ATLAS_RETRIEVAL_SCOPE','project')
        # clusters name
        self.CLUSTERS_NAME  = self.config.get('CLUSTERS_NAME',[])
        # processId
        self.PROCESSES_ID = self.config.get('PROCESSES_ID',[])
        self.INPUT_PATH = self.config.get('INPUT_PATH' , 'inputs')
        createDirs(self.INPUT_PATH)
        self.REPORT_FILE_PATH = self.config.get('REPORT_FILE_PATH','reports')
        createDirs(self.REPORT_FILE_PATH)
        self.OUTPUT_FILE_PATH = self.config.get('OUTPUT_FILE_PATH','outputs')
        #none parquet json
        self.SAVE_BY_CHUNK = self.config.get('SAVE_BY_CHUNK', 'json')
        self.MAX_CHUNK_SIZE = self.config.get('MAX_CHUNK_SIZE',50000)
        createDirs(self.OUTPUT_FILE_PATH)
        self.LOGS_FILENAME = self.config.get('LOGS_FILENAME',['mongodb.log']) # was LOG_FILE_PATH
        self.GENERATE_ONE_PDF_PER_CLUSTER_FILE = self.config.get('GENERATE_ONE_PDF_PER_CLUSTER_FILE',True)
        self.GENERATE_SLOW_QUERY_LOG = self.config.get('GENERATE_SLOW_QUERY_LOG',True)
        self.GENERATE_PDF_REPORT = self.config.get('GENERATE_PDF_REPORT',True)
        self.GENERATE_MD = self.config.get('GENERATE_MD',False)
        self.GENERATE_PNG = self.config.get('GENERATE_PNG',False)
        self.DELETE_IMAGE_AFTER_USED = self.config.get('DELETE_IMAGE_AFTER_USED',False)
        # one of MongoDB or files
        self.MONGO_RETRIEVAL_MODE = self.config.get('MONGO_RETRIEVAL_MODE','files')
        self.MONGO_CRED = self.config.get('MONGO_CRED',None)
        self.MINIMUM_DURATION_FOR_QUERYSHAPE = self.config.get('MINIMUM_DURATION_FOR_QUERYSHAPE',0)
        self.INSERT_GRAPH_SUMMARY_TO_REPORT =self.config.get('INSERT_GRAPH_SUMMARY_TO_REPORT',True)
        #self.ADD_MAX_QUERY =  self.config.get('ADD_MAX_QUERY',True)
        print(matplotlib.rcsetup.all_backends)
        list(colormaps)

        if self.GENERATE_PNG:
            matplotlib.use("cairo")
        else:
            matplotlib.use("svg")

        if self.RETRIEVAL_MODE == "Atlas":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with pub key:{self.PUBLIC_KEY}")
        elif self.RETRIEVAL_MODE == "files":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with input_path : {self.INPUT_PATH}  logs:{self.LOGS_FILENAME}")
        elif self.RETRIEVAL_MODE == "OpsManager":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with pub key:{self.PUBLIC_KEY}")
        else:
            raise ValueError(f"Unsupported RETRIEVAL_MODE: {self.RETRIEVAL_MODE}")

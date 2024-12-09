import json


# Load configuration from a JSON file
def load_config(file_path):
    print(f"loading config from {file_path}")
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

class Config():
    def __init__(self, configName):
        # Load the configuration
        if configName:
            self.config = load_config(f"config/{configName}.json")
        else:
            self.config = load_config(f"config/config.json")
        # Access configuration values
        self.RETRIEVAL_MODE = self.config.get('RETRIEVAL_MODE', 'files')
        self.PUBLIC_KEY = self.config.get('PUBLIC_KEY',None)
        self.PRIVATE_KEY = self.config.get('PRIVATE_KEY',None)
        self.GROUP_ID = self.config.get('GROUP_ID',None)
        self.PROCESSES_ID = self.config.get('PROCESSES_ID',[])
        self.INPUT_PATH = self.config.get('INPUT_PATH' , 'inputs')
        self.LOGS_FILENAME = self.config.get('LOGS_FILENAME',['mongodb.log']) # was LOG_FILE_PATH
        self.OUTPUT_FILE_PATH = self.config['OUTPUT_FILE_PATH']
        self.REPORT_FILE_PATH = self.config.get('REPORT_FILE_PATH','reports')
        self.GENERATE_SLOW_QUERY_LOG = self.config.get('GENERATE_SLOW_QUERY_LOG',True)
        self.GENERATE_PDF_REPORT = self.config.get('GENERATE_PDF_REPORT',True)
        self.GENERATE_MD = self.config.get('GENERATE_MD',False)
        if self.RETRIEVAL_MODE == "Atlas":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with pub key:{self.PUBLIC_KEY}")
        elif self.RETRIEVAL_MODE == "files":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with input_path : {self.INPUT_PATH}  logs:{self.LOGS_FILENAME}")
        elif self.RETRIEVAL_MODE == "OpsManager":
            print(f"Retrieval Mode: {self.RETRIEVAL_MODE} with pub key:{self.PUBLIC_KEY}")
        else:
            raise ValueError(f"Unsupported RETRIEVAL_MODE: {self.RETRIEVAL_MODE}")

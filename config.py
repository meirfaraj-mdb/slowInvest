import json


# Load configuration from a JSON file
def load_config(file_path):
    print(f"loading config from config/{file_path}.json")
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

class Config():
    def __init__(self,configName):
        # Load the configuration
        if configName :
          self.config = load_config(f"config/{configName}.json")
        else:
          self.config = load_config(f"config/config.json")
        # Access configuration values
        self.USING_API = self.config['USING_API']
        self.PUBLIC_KEY = self.config['PUBLIC_KEY']
        self.PRIVATE_KEY = self.config['PRIVATE_KEY']
        self.GROUP_ID = self.config['GROUP_ID']
        self.PROCESSES_ID = self.config['PROCESSES_ID']
        self.LOG_FILE_PATH = self.config['LOG_FILE_PATH']
        self.OUTPUT_FILE_PATH = self.config['OUTPUT_FILE_PATH']
        self.GENERATE_SLOW_QUERY_LOG = self.config['GENERATE_SLOW_QUERY_LOG']
        self.GENERATE_PDF_REPORT = self.config['GENERATE_PDF_REPORT']
        self.GENERATE_MD = self.config['GENERATE_MD']
        # Now you can use these variables in your script
        print(f"Using API: {self.USING_API}")
        print(f"Public Key: {self.PUBLIC_KEY}")

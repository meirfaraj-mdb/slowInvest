import gc
import matplotlib.style as mplstyle
import matplotlib
import msgspec
import logging

from sl_utils.utils import createDirs

decoder = msgspec.json.Decoder()
logging.basicConfig(level=logging.DEBUG)

# Load configuration from a JSON file
def load_config(file_path):
    print(f"loading config from {file_path}")
    with open(file_path, 'r') as config_file:
        config = decoder.decode(config_file.read())
    return config

class Config():

    def __init__(self, configName):
        self.default = load_config("config/default/default.json")
        self.default_template = load_config("config/default/template.json")

        createDirs("logs/")
        createDirs("reports/")
        createDirs("inputs/")
        createDirs("reports/")

        logging.basicConfig(
            level=logging.DEBUG,  # Set desired level (e.g., INFO, DEBUG, WARNING)
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("logs/logApp.log"),  # Log to specified file
                logging.StreamHandler()  # Optionally log to console as well
            ]
        )
        # Load the configuration
        if configName:
            self.config = load_config(f"config/{configName}.json")
        else:
            self.config = load_config("config/config.json")
        self.template_name = self.get_config('template', None)
        if self.template_name is None:
            self.template = self.default_template
        else:
            self.template = load_config(f"config/templates/{self.template_name}.json")

        # Atlas / files / OpsManager
        self.retrieval_mode = self.get_config('retrieval_mode', 'files')
        # Atlas/Ops Manager related
        self.PUBLIC_KEY = self.get_config('PUBLIC_KEY',None)
        self.PRIVATE_KEY = self.get_config('PRIVATE_KEY',None)
        self.GROUP_ID = self.get_config('GROUP_ID',None)
        # one of project, clusters, processId
        self.ATLAS_RETRIEVAL_SCOPE = self.get_config('ATLAS_RETRIEVAL_SCOPE','project')
        # clusters name
        self.CLUSTERS_NAME  = self.get_config('CLUSTERS_NAME',[])
        # processId
        self.PROCESSES_ID = self.get_config('PROCESSES_ID',[])
        self.INPUT_PATH = self.get_config('INPUT_PATH' , 'inputs')
        createDirs(self.INPUT_PATH)
        self.REPORT_FILE_PATH = self.get_config('REPORT_FILE_PATH','reports')
        createDirs(self.REPORT_FILE_PATH)
        self.OUTPUT_FILE_PATH = self.get_config('OUTPUT_FILE_PATH','outputs')
        #none parquet json
        self.SAVE_BY_CHUNK = self.get_config('SAVE_BY_CHUNK', 'json')
        self.MAX_CHUNK_SIZE = self.get_config('MAX_CHUNK_SIZE',50000)
        createDirs(self.OUTPUT_FILE_PATH)
        self.LOGS_FILENAME = self.get_config('LOGS_FILENAME',['mongodb.log']) # was LOG_FILE_PATH
        self.GENERATE_ONE_PDF_PER_CLUSTER_FILE = self.get_config('GENERATE_ONE_PDF_PER_CLUSTER_FILE',True)
        self.GENERATE_SLOW_QUERY_LOG = self.get_config('GENERATE_SLOW_QUERY_LOG',True)
        self.GENERATE_MD = self.get_config('GENERATE_MD',False)
        self.GENERATE_PNG = self.get_config('GENERATE_PNG',False)
        self.DELETE_IMAGE_AFTER_USED = self.get_config('DELETE_IMAGE_AFTER_USED',False)
        # one of MongoDB or files
        self.MONGO_RETRIEVAL_MODE = self.get_config('MONGO_RETRIEVAL_MODE','files')
        self.MONGO_CRED = self.get_config('MONGO_CRED',None)
        self.MINIMUM_DURATION_FOR_QUERYSHAPE = self.get_config('MINIMUM_DURATION_FOR_QUERYSHAPE',0)
        self.INSERT_GRAPH_SUMMARY_TO_REPORT =self.get_config('INSERT_GRAPH_SUMMARY_TO_REPORT',True)
        #self.ADD_MAX_QUERY =  self.get_config('ADD_MAX_QUERY',True)
        self.GENERATE_INFRA_REPORT = self.get_config('GENERATE_INFRA_REPORT',True)

        self.GENERATE_ORIG_FILE_ONLY = self.get_config('GENERATE_ORIG_FILE_ONLY',False)
        if self.GENERATE_ORIG_FILE_ONLY:
            self.GENERATE_MD = False
            self.GENERATE_INFRA_REPORT=False
            self.GENERATE_PNG =False
            self.INSERT_GRAPH_SUMMARY_TO_REPORT=False

        if self.GENERATE_PNG:
            matplotlib.use("cairo")
        else:
            matplotlib.use("svg")
            mplstyle.use('fast')
        if self.retrieval_mode == "Atlas":
            print(f"Retrieval Mode: {self.retrieval_mode} with pub key:{self.PUBLIC_KEY}")
        elif self.retrieval_mode == "files":
            print(f"Retrieval Mode: {self.retrieval_mode} with input_path : {self.INPUT_PATH}  logs:{self.LOGS_FILENAME}")
        elif self.retrieval_mode == "OpsManager":
            print(f"Retrieval Mode: {self.retrieval_mode} with pub key:{self.PUBLIC_KEY}")
        else:
            raise ValueError(f"Unsupported retrieval_mode: {self.retrieval_mode}")
        gc.collect(2)
        gc.freeze()
        allocs,gen1,gen2=gc.get_threshold()
        allocs=300_000
        gen1*=5
        #gen2*=2
        gc.set_threshold(allocs,gen1,gen2)


    def get(self,type,config,default, name, default_val):
        def get_nested(dic, path):
            keys = path.split('.')
            current = dic
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current

            # Try nested lookups via dot notation
        result = get_nested(config, name)
        if result is None:
            # Fall back to default config, dotted path
            result = get_nested(default, name)
        if result is None:
            # Last resort: try direct key (in case full name)
            result = config.get(name, default.get(name, default_val))
        logging.debug(f"[%s] %s = %s",type, name, result)
        return result

    def get_config(self, name, default_val):
        return self.get("config",self.config,self.default, name, default_val)

    def get_template(self, name, default_val):
        return self.get("template",self.template,self.default_template, name, default_val)

    def get_fields_array(self,path):
        result = []
        fields = self.get("template",self.default_template,self.default_template, path, [])

        for field_key, field_info in fields.items():
            base_path = path+"."+field_key
            if self.get_template(base_path+".include", True) :
                result.append([self.get_template(base_path+".title","N/A"),
                               self.get_template(base_path+".path","N/A")])
        return result

    def get_report_formats(self):
        if self.GENERATE_ORIG_FILE_ONLY:
           return []
        return self.get_config("reports",
                           self.default.get("reports",{})).get("formats",["pdf"])
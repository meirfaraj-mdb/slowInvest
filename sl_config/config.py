import gc
import matplotlib.style as mplstyle
import matplotlib
import msgspec
import logging
import os

from sl_utils.utils import createDirs

# JSON decoder
decoder = msgspec.json.Decoder()

# Initial basic logging
logging.basicConfig(level=logging.DEBUG)


# ----------- Utility to load config with error handling ----------
def load_config(file_path):
    """
    Load JSON config safely. Returns {} if file missing or invalid.
    Logs errors and stack traces for debugging.
    """
    print(f"Loading config from {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"Config file not found: {file_path}")
        return {}
    try:
        with open(file_path, 'r') as config_file:
            config = decoder.decode(config_file.read())
        if not isinstance(config, dict):
            logging.error(f"Invalid config format in: {file_path}")
            return {}
        return config
    except Exception as e:
        logging.exception(f"Error loading config file {file_path}: {e}")
        return {}


    # ------------------------------ Main Config Class ------------------------------
class Config:
    # Mapping of old config keys/paths to new paths
    RENAMED_PATHS = {
        "PUBLIC_KEY" : "atlas.PUBLIC_KEY",
        "PRIVATE_KEY": "atlas.PRIVATE_KEY",
        "GROUP_ID": "atlas.GROUP_ID",
        'ATLAS_RETRIEVAL_SCOPE':"atlas.retrieval_scope",
        "LIMIT_QUERYSHAPE" : "reports.top_slow_query_shape",
        "MINIMUM_DURATION_FOR_QUERYSHAPE" : "reports.min_duration_slow_query_shape",
        "GROUP_BY_ISSUE" : "reports.group_by_issue",
        "logFilePath": "LOGS_FILENAME",
        "outputDir": "OUTPUT_FILE_PATH",
        "mongoCredentials": "MONGO_CRED",
    }

    def __init__(self, configName):
        # Load defaults
        self.default = load_config("config/default/default.json")
        self.default_template = load_config("config/default/template.json")

        # Ensure required directories exist early
        for dir_path in ["logs/", "reports/", "inputs/", "outputs/"]:
            createDirs(dir_path)

            # Configure logging once
        self._configure_logging()

        # Load user-provided config
        if configName:
            self.config = load_config(f"config/{configName}.json")
        else:
            self.config = load_config("config/config.json")

            # 🚨 Check for renamed paths in both user & defaults
        self._check_renamed_paths(self.config)
        self._check_renamed_paths(self.default)

        # Load template config
        self.template_name = self.get_config('template', None)
        if self.template_name:
            self.template = load_config(f"config/templates/{self.template_name}.json")
        else:
            self.template = self.default_template

            # ---------------- Retrieval & MongoDB settings ----------------
        self.retrieval_mode = self.get_config('retrieval_mode', 'files')
        self.PUBLIC_KEY = self.get_config('atlas.PUBLIC_KEY', None)
        self.PRIVATE_KEY = self.get_config('atlas.PRIVATE_KEY', None)
        self.GROUP_ID = self.get_config('atlas.GROUP_ID', None)
        self.ATLAS_RETRIEVAL_SCOPE = self.get_config("atlas.retrieval_scope", 'project')
        self.CLUSTERS_NAME = self._validate_type(self.get_config('CLUSTERS_NAME', []), list, [])
        self.PROCESSES_ID = self._validate_type(self.get_config('PROCESSES_ID', []), list, [])

        # ---------------- Path & directory creation from config ----------------
        self.INPUT_PATH = self.get_config('INPUT_PATH', 'inputs')
        createDirs(self.INPUT_PATH)
        self.REPORT_FILE_PATH = self.get_config('REPORT_FILE_PATH', 'reports')
        createDirs(self.REPORT_FILE_PATH)
        self.OUTPUT_FILE_PATH = self.get_config('OUTPUT_FILE_PATH', 'outputs')
        createDirs(self.OUTPUT_FILE_PATH)

        # ---------------- Data save options ----------------
        self.SAVE_BY_CHUNK = self.get_config('SAVE_BY_CHUNK', 'json')
        self.MAX_CHUNK_SIZE = self._validate_type(self.get_config('MAX_CHUNK_SIZE', 50000), int, 50000)

        # ---------------- Report generation flags ----------------
        self.LOGS_FILENAME = self._validate_type(self.get_config('LOGS_FILENAME', ['mongodb.log']), list, ['mongodb.log'])
        self.GENERATE_ONE_PDF_PER_CLUSTER_FILE = self._validate_type(self.get_config('GENERATE_ONE_PDF_PER_CLUSTER_FILE', True), bool, True)
        self.GENERATE_SLOW_QUERY_LOG = self._validate_type(self.get_config('GENERATE_SLOW_QUERY_LOG', True), bool, True)
        self.GENERATE_MD = self._validate_type(self.get_config('GENERATE_MD', False), bool, False)
        self.GENERATE_PNG = self._validate_type(self.get_config('GENERATE_PNG', False), bool, False)
        self.DELETE_IMAGE_AFTER_USED = self._validate_type(self.get_config('DELETE_IMAGE_AFTER_USED', False), bool, False)
        self.MONGO_RETRIEVAL_MODE = self.get_config('MONGO_RETRIEVAL_MODE', 'files')
        self.MONGO_CRED = self.get_config('MONGO_CRED', None)
        self.MINIMUM_DURATION_FOR_QUERYSHAPE = self._validate_type(self.get_config("reports.min_duration_slow_query_shape", 0), int, 0)
        self.LIMIT_QUERYSHAPE = self._validate_type(self.get_config("reports.top_slow_query_shape", 0), int, 0)
        self.INSERT_GRAPH_SUMMARY_TO_REPORT = self._validate_type(self.get_config('INSERT_GRAPH_SUMMARY_TO_REPORT', True), bool, True)
        self.GENERATE_INFRA_REPORT = self._validate_type(self.get_config('GENERATE_INFRA_REPORT', True), bool, True)
        self.GROUP_BY_ISSUE = self._validate_type(self.get_config('reports.group_by_issue', False), bool, False)

        self.GENERATE_ORIG_FILE_ONLY = self._validate_type(self.get_config('GENERATE_ORIG_FILE_ONLY', False), bool, False)
        if self.GENERATE_ORIG_FILE_ONLY:
            self.GENERATE_MD = False
            self.GENERATE_INFRA_REPORT = False
            self.GENERATE_PNG = False
            self.INSERT_GRAPH_SUMMARY_TO_REPORT = False

            # ---------------- Matplotlib backend selection ----------------
        if self.GENERATE_PNG:
            matplotlib.use("cairo")
        else:
            matplotlib.use("svg")
            mplstyle.use('fast')

            # ---------------- Retrieval mode logging ----------------
        if self.retrieval_mode == "Atlas":
            print(f"Retrieval Mode: {self.retrieval_mode} with pub key: {self.PUBLIC_KEY}")
        elif self.retrieval_mode == "files":
            print(f"Retrieval Mode: {self.retrieval_mode} with input_path: {self.INPUT_PATH} logs: {self.LOGS_FILENAME}")
        elif self.retrieval_mode == "OpsManager":
            print(f"Retrieval Mode: {self.retrieval_mode} with pub key: {self.PUBLIC_KEY}")
        else:
            raise ValueError(f"Unsupported retrieval_mode: {self.retrieval_mode}")

            # ---------------- Optional GC tuning ----------------
        if self._validate_type(self.get_config('ENABLE_GC_TUNING', True), bool, True):
            self._configure_gc()

            # ---------------- Rename path check ----------------
    def _check_renamed_paths(self, config_dict):
        """Stop execution if any old config path is found."""
        def get_nested(dic, path):
            keys = path.split('.')
            current = dic
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current

        found_old_paths = []
        for old_path, new_path in self.RENAMED_PATHS.items():
            if get_nested(config_dict, old_path) is not None:
                found_old_paths.append((old_path, new_path))

        if found_old_paths:
            error_msg_lines = [
                "❌ Your configuration file contains outdated keys:",
                *[f" - '{old}' → should be renamed to '{new}'" for old, new in found_old_paths],
                "Please update your config JSON to use the new paths before running again."
            ]
            raise ValueError("\n".join(error_msg_lines))

            # ---------------- Logging config ----------------
    def _configure_logging(self):
        logging.getLogger().handlers.clear()  # Avoid duplicate handlers
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("logs/logApp.log"),
                logging.StreamHandler()
            ]
        )

        # ---------------- GC tuning ----------------
    def _configure_gc(self):
        gc.collect(2)
        gc.freeze()
        allocs, gen1, gen2 = gc.get_threshold()
        allocs = 300_000
        gen1 *= 5
        gc.set_threshold(allocs, gen1, gen2)

        # ---------------- Type validation ----------------
    def _validate_type(self, value, expected_type, default):
        """Ensure config values match expected type, else return default."""
        if isinstance(value, expected_type):
            return value
        logging.warning(f"Type mismatch: expected {expected_type.__name__}, got {type(value).__name__}. Using default={default}")
        return default

        # ---------------- General access helpers ----------------
    def get(self, type, config, default, name, default_val):
        def get_nested(dic, path):
            keys = path.split('.')
            current = dic
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current

        result = get_nested(config, name)
        if result is None:
            result = get_nested(default, name)
        if result is None:
            result = config.get(name, default.get(name, default_val))
        logging.debug(f"[{type}] {name} = {result}")
        return result

    def get_config(self, name, default_val):
        return self.get("config", self.config, self.default, name, default_val)

    def get_template(self, name, default_val):
        return self.get("template", self.template, self.default_template, name, default_val)

    def get_fields_array(self, path):
        result = []
        fields = self.get("template", self.default_template, self.default_template, path, [])
        if fields:
            for field_key, field_info in fields.items():
                base_path = path + "." + field_key
                if self.get_template(base_path + ".include", True):
                    result.append([
                        self.get_template(base_path + ".title", "N/A"),
                        self.get_template(base_path + ".path", "N/A")
                    ])
        return result

    def get_report_formats(self):
        if self.GENERATE_ORIG_FILE_ONLY:
            return []
        return self.get_config("reports",
                               self.default.get("reports", {})).get("formats", ["pdf"])

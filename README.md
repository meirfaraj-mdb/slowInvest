# slowInvest


-----------------------------
## Disclaimer
This software is a personal analysis experiment and is NOT related to MongoDB, Inc.
It is not covered by any commercial support subscriptions or services from MongoDB, Inc.
Use of slowInvest is at your own risk.

-----------------------------
## Purpose
This project is an experimental set of Python scripts designed to streamline the analysis
of slow queries by organizing them based on query shape and categorizing them accordingly.
The project is being expanded to include new features such as a configuration report,
scaling event analysis, an alert section, and customizable alerts specifically for scaling issues.

-----------------------------
## Prerequisite

* Requires Python 3.13 or later.
* May work with Python 3.11 or later. 
-----------------------------
## Usage

python finalSlowInvest [configName]
where configName is the config name for the config json to load without json by default config.

Command: python finalSlowInvest [configName]

### Command: `python finalSlowInvest [configName]`

#### Description

Executes the `finalSlowInvest` script using the specified configuration.

- **`[configName]`**: This argument refers to the name of the configuration JSON file to be loaded, excluding the `.json` extension.

- **Default Behavior**: If no `[configName]` is provided, the script will use a default configuration named `config`.

### Configuration Options

This document outlines the configuration options available for the application.
These configurations determine the behavior and functionality of the data retrieval and report generation processes.

#### Atlas / Files / OpsManager

- **`retrieval_mode`**: Specifies the mode for data retrieval. Options are:
    - `Atlas`
    - `files` (default)
    - `OpsManager`

#### Atlas / Ops Manager Related

- **`PUBLIC_KEY`**: Public key used for authentication. Default is `None`.
- **`PRIVATE_KEY`**: Private key used for authentication. Default is `None`.
- **`GROUP_ID`**: The group ID or project ID used for identifying the target group in Atlas/Ops Manager. Default is `None`.

#### Retrieval Scope

- **`ATLAS_RETRIEVAL_SCOPE`**: Defines the scope of data retrieval in Atlas. Options are:
    - `project` (default)
    - `clusters`
    - `processId`

#### Clusters and Processes

- **`CLUSTERS_NAME`**: A list of cluster names to include in the retrieval process. Default is an empty list (`[]`).
- **`PROCESSES_ID`**: A list of process IDs to include in the retrieval. Default is an empty list (`[]`).

#### Paths and Directories

- **`INPUT_PATH`**: Directory path for input files. Default is `'inputs'`.
- **`REPORT_FILE_PATH`**: Directory path where report files are saved. Default is `'reports'`.
- **`OUTPUT_FILE_PATH`**: Directory path where output files are saved. Default is `'outputs'`.

#### Logging and Reporting

- **`LOGS_FILENAME`**: List of log file names to process. Default is `['mongodb.log']`.
- **`GENERATE_ONE_PDF_PER_CLUSTER_FILE`**: Boolean indicating whether to generate a separate PDF report for each cluster file. Default is `True`.
- **`GENERATE_SLOW_QUERY_LOG`**: Boolean indicating whether to generate a slow query log. Default is `True`.
- **`GENERATE_MD`**: Boolean indicating whether to generate a Markdown report. Default is `False`.
- **`GENERATE_PNG`**: Boolean indicating whether to generate PNG images. Default is `False`.

##### Reporting
- **`reports.formats`** list of formats to generate. Default is `[pdf]`
  Available formats : pdf,md
- `pdf`**: generate a PDF report.

#### Cleanup Options

- **`DELETE_IMAGE_AFTER_USED`**: Boolean indicating whether to delete images after they have been used in the report. Default is `False`.

#### Query and Report Settings

- **`MINIMUM_DURATION_FOR_QUERYSHAPE`**: Minimum duration (in seconds) for considering a query shape in reports. Default is `0`.
- **`INSERT_GRAPH_SUMMARY_TO_REPORT`**: Boolean indicating whether to insert a graph summary into the report. Default is `True`.

---

Each configuration option can be overridden in the application's configuration file.

### Template option

Using Custom Templates for Personalized Reports
You can now use a template to generate a personalized report.
Documentation for the fields and titles that can be overridden is available here : [Template Documentation](template_doc.md)


Default and Custom Templates
The default template is located at:

/config/default/template.json  
To create your own template:

Place your template file under:
/config/templates/  
Reference the template by its name (without the .json extension) in your configuration, like so:
"template": "template_name"

(For example, to use audit_report.json, set "template": "audit_report")
Examples
Example template:
/config/templates/audit_report.json  
Example configuration using the custom template:
/config/audit_report.json




-----------------------------
python finalSlowInvest --server

The --server option launches the program as a web server, providing a web-based interface.

Command: python finalSlowInvest --server

### Command: `python finalSlowInvest --server`

#### Description

The --server option launches the program as a web server, providing a web-based interface.
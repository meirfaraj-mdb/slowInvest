# slowInvest


-----------------------------
## Disclaimer
This software is a personal analysis experiment and is NOT related to MongoDB, Inc. It is not covered by any commercial support subscriptions or services from MongoDB, Inc. Use of slowInvest is at your own risk.

-----------------------------
## Purpose
This project is an experimental collection of Python scripts that simplify the review of slow queries by grouping them according to query shape and categorizing them.

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

-----------------------------
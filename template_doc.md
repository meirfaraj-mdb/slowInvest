# JSON Template Documentation

**Title:** Slow Query Report

## Top-Level Config
- **include_toc**: True
- **initial_empty_page**: True

## Format
```json
{
  "title": {
    "level_0": {
      "font_family": "Times",
      "font_style": "B",
      "font_size_pt": 16,
      "color": 128,
      "underline": true,
      "t_margin": 0,
      "l_margin": 2,
      "b_margin": 1
    },
    "level_1": {
      "font_family": "Times",
      "font_style": "B",
      "font_size_pt": 14,
      "color": 128,
      "underline": true,
      "t_margin": 0,
      "l_margin": 10,
      "b_margin": 1
    },
    "level_2": {
      "font_family": "Times",
      "font_style": "B",
      "font_size_pt": 12,
      "color": 128,
      "underline": true,
      "t_margin": 0,
      "l_margin": 4,
      "b_margin": 1
    },
    "level_3": {
      "font_family": "Times",
      "font_style": "B",
      "font_size_pt": 8,
      "color": 128,
      "underline": true,
      "t_margin": 0,
      "l_margin": 6,
      "b_margin": 1
    },
    "level_4": {
      "font_family": "Times",
      "font_style": "",
      "font_size_pt": 5,
      "color": 128,
      "underline": true,
      "t_margin": 0,
      "l_margin": 8,
      "b_margin": 1
    }
  }
}
```

## Sections

## config

**Included:** `True`

### General

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `cluster_name` | Cluster Name | `name` | True |
| `cluster_type` | Cluster Type | `clusterType` | True |
| `create_date` | Create Date | `createDate` | True |
| `feature_compatibility_version` | Feature Compatibility Version | `featureCompatibilityVersion` | True |
| `mongodb_major_version` | MongoDB Major Version | `mongoDBMajorVersion` | True |
| `mongodb_version` | MongoDB Version | `mongoDBVersion` | True |
| `version_release_system` | Version Release System | `versionReleaseSystem` | True |
| `group_id` | Group/Project Id | `groupId` | True |
| `cluster_id` | Cluster Id | `id` | True |
| `paused` | Paused | `paused` | True |
| `termination_protection_enabled` | Termination Protection Enabled | `terminationProtectionEnabled` | True |
| `bi_connector` | BI Connector | `biConnector` | True |
| `tags` | Tags | `tags` | True |
| `labels` | Labels | `labels` | True |
| `config_server_management_mode` | Config Server Management Mode | `configServerManagementMode` | True |
| `config_server_type` | Config Server Type | `configServerType` | True |
| `global_cluster_self_managed_sharding` | Global Cluster Self Managed Sharding | `globalClusterSelfManagedSharding` | True |
| `disk_warming_mode` | Disk Warming Mode | `diskWarmingMode` | True |
| `encryption_at_rest_provider` | Encryption At Rest Provider | `encryptionAtRestProvider` | True |
| `root_cert_type` | Root Cert Type | `rootCertType` | True |
| `redact_client_log_data` | Redact Client Log Data | `redactClientLogData` | True |
| `instance_composition` | Instance Composition | `instance_composition` | True |
| `Providers` | Providers | `providers` | True |
| `providers_count` | Providers count | `providers_count` | True |
| `regions` | Regions | `regions` | True |
| `regions_count` | Regions count | `regions_count` | True |
| `backup_enabled` | Backup Enabled | `backupEnabled` | True |
| `pitr_enabled` | PITR Enabled | `pitEnabled` | True |
| `backup_compliance_configured` | Backup Compliance configured | `backupCompliance_configured` | True |
| `backup_snapshot_count` | Backup snapshot count | `backup_snapshot_count` | True |
| `online_archive_count` | Online Archive Count | `onlineArchiveForOneCluster_count` | True |
| `suggested_index_count` | Suggested Index Count | `performanceAdvisorSuggestedIndexes_count` | True |

### Advanced configuration

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `javascript_enabled` | Javascript Enabled | `javascriptEnabled` | True |
| `minimum_enabled_tls_protocol` | minimum enabled Tls Protocol | `minimumEnabledTlsProtocol` | True |
| `query stats_log_verbosity` | query stats log verbosity | `queryStatsLogVerbosity` | True |
| `tls_cipher_config_mode` | tls cipher config mode | `tlsCipherConfigMode` | True |
| `change_stream_options_pre_and_post_images_expire_after_seconds` | Pre+PostImages Expire After Seconds | `changeStreamOptionsPreAndPostImagesExpireAfterSeconds` | True |
| `chunk_migration_concurrency` | chunk Migration Concurrency | `chunkMigrationConcurrency` | True |
| `custom_openssl_cipher_config_tls12` | custom Openssl Cipher Config Tls12 | `customOpensslCipherConfigTls12` | True |
| `default_maxtime_ms` | default Max Time MS | `defaultMaxTimeMS` | True |
| `default_write_concern` | default Write Concern | `defaultWriteConcern` | True |
| `no_table_scan` | no table scan | `noTableScan` | True |
| `oplog_min_retention_hours` | oplog min retention hours | `oplogMinRetentionHours` | True |
| `oplog_size_mb` | oplog Size MB | `oplogSizeMB` | True |
| `sample_refresh_interval_bi_connector` | sample Refresh Interval BI Connector | `sampleRefreshIntervalBIConnector` | True |
| `sample_size_bi_connector` | sample Size BI Connector | `sampleSizeBIConnector` | True |
| `transaction_lifetime_limit_seconds` | transaction Lifetime Limit Seconds | `transactionLifetimeLimitSeconds` | True |

### Backup Compliance

**Included:** `True`

### Replication specs

**Included:** `True`

#### Electable specs

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `instanceSize` | Instance Size (tiers) | `instanceSize` | True |
| `diskIOPS` | Disk IOPS | `diskIOPS` | True |
| `diskSizeGB` | Disk Size (GB) | `diskSizeGB` | True |
| `ebsVolumeType` | Volume Type | `ebsVolumeType` | True |
| `nodeCount` | Node Count | `nodeCount` | True |

#### Read Only specs

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `instanceSize` | Instance Size (tiers) | `instanceSize` | True |
| `diskIOPS` | Disk IOPS | `diskIOPS` | True |
| `diskSizeGB` | Disk Size (GB) | `diskSizeGB` | True |
| `ebsVolumeType` | Volume Type | `ebsVolumeType` | True |
| `nodeCount` | Node Count | `nodeCount` | True |

#### Analytic specs

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `instanceSize` | Instance Size (tiers) | `instanceSize` | True |
| `diskIOPS` | Disk IOPS | `diskIOPS` | True |
| `diskSizeGB` | Disk Size (GB) | `diskSizeGB` | True |
| `ebsVolumeType` | Volume Type | `ebsVolumeType` | True |
| `nodeCount` | Node Count | `nodeCount` | True |

##### Analytic Autoscaling

**Included:** `True`

| Field | Title | Path | Included |
|---|---|---|---|
| `diskGB_enabled` | Disk autoscaling Enabled | `diskGB.enabled` | True |
| `compute_enabled` | Compute autoscaling Enabled | `compute.enabled` | True |
| `compute_minInstanceSize` | Compute min Tiers | `compute.minInstanceSize` | True |
| `compute_maxInstanceSize` | Compute max Tiers | `compute.maxInstanceSize` | True |
| `compute_scaleDownEnabled` | Compute scale Down Enabled | `compute.scaleDownEnabled` | True |
| `autoIndexing` | Compute scale Down Enabled | `compute.scaleDownEnabled` | True |

## cluster

### per_node

#### graph

##### group_of_metrics

###### ASSERTIONS

###### CACHE

###### CONNECTIONS_AND_CURSORS

###### STORAGE

###### DOCUMENT_ACTIVITY

###### PAGE_FAULTS

###### GLOBAL_LOCK

###### MEMORY

###### NETWORK

###### OPCOUNTERS_PRIMARY

###### OPCOUNTERS_REPL

###### OPERATIONS

###### OP_EXECUTION_TIMES

###### OPLOG

###### QUERY_EXECUTION

###### READ_WRITE_TICKETS

###### OPERATION_THROTTLING

###### QUERY_SPILL_SORT

###### PROCESS_CPU

###### FTS_PROCESS

###### SYSTEM_CPU

###### SYSTEM_NETWORK

###### SWAP_IO

###### SYSTEM_MEMORY

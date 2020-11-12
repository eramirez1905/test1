CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.dataset }}.{{ params.view_name }}` AS
WITH billing_dataset AS (
  SELECT
    DATE(timestamp) AS ReportDate,
    timestamp AS Date,
    resource.labels.project_id AS ProjectId,
    protopayload_auditlog.serviceName AS ServiceName,
    protopayload_auditlog.methodName AS MethodName,
    protopayload_auditlog.status.code AS StatusCode,
    protopayload_auditlog.status.message AS StatusMessage,
    CASE
      WHEN STARTS_WITH(protopayload_auditlog.requestMetadata.callerSuppliedUserAgent, 'Tableau')
        THEN 'tableau'
      WHEN STARTS_WITH(protopayload_auditlog.requestMetadata.callerSuppliedUserAgent, 'Metabase')
        THEN 'metabase'
      ELSE protopayload_auditlog.authenticationInfo.principalEmail
    END AS UserId,
    (SELECT COALESCE(value, 'NO_AIRFLOW') FROM UNNEST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels) WHERE key = 'dag_id') AS AirflowDagId,
    (SELECT COALESCE(value, 'NO_AIRFLOW') FROM UNNEST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels) WHERE key = 'task_id') AS AirflowTaskId,
    (SELECT COALESCE(value, 'NO_BUSINESS_UNIT') FROM UNNEST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels) WHERE key = 'business_unit') AS AirflowBusinessUnit,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId AS JobId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS Query,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.projectId AS TableProjectId,
    IF(
      REGEXP_CONTAINS(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId, r'curated_data_v[0-9]_stream_table_(.+)_[0-9]{6,}T[0-9]{1,}'),
      'cl',
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.datasetId
    ) AS Dataset,
    COALESCE(
      IF(
        REGEXP_CONTAINS(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId, r'curated_data_v[0-9]_stream_table_(.+)_[0-9]{6,}T[0-9]{1,}'),
        REGEXP_REPLACE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId, r'curated_data_v[0-9]_stream_table_(.+)_[0-9]{6,}T[0-9]{1,}', '\\1'),
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.destinationTable.tableId
      ),
      REGEXP_EXTRACT(protopayload_auditlog.resourceName, '[^/]+$')
    ) AS Table,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS CreateDisposition,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.writeDisposition AS WriteDisposition,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.dryRun AS DryRun,
  	protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels AS Labels,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.state AS JobState,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code AS JobErrorCode,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message AS JobErrorMessage,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime AS JobCreateTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS JobStartTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime AS JobEndTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.reservationUsage,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.billingTier AS BillingTier,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes AS TotalBilledBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes AS TotalProcessedBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / POW(2, 30) AS TotalBilledGigabytes,
    (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / POW(2, 40)) AS TotalBilledTerabytes,
    (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / POW(2, 40)) * 5 AS TotalCost,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.TotalSlotMS,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
    1 AS Queries
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.source_table_name }}`
WHERE protopayload_auditlog.serviceName = 'bigquery.googleapis.com'
  AND protopayload_auditlog.methodName = 'jobservice.jobcompleted'
  AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName = 'query_job_completed'
), billing_cleansed AS (
  SELECT * EXCEPT(Dataset, Table)
    , totalSlotMs / TIMESTAMP_DIFF(JobEndTime, JobStartTime, MILLISECOND) AS AverageSlotsUsed
    , Dataset AS DatasetOriginal
    , Table AS TableOriginal
    , CASE
        WHEN REGEXP_CONTAINS(Dataset, r"^anon[0-9a-z]{8}_[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{12}$") THEN 'anonymous'
        WHEN REGEXP_CONTAINS(Dataset, r"^_[0-9a-z]{40}") THEN 'temporary'
        ELSE Dataset
      END AS Dataset
    , CASE
        WHEN REGEXP_CONTAINS(Table, r"^anon[0-9a-z]{8}_[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{12}$") THEN 'anonymous'
        WHEN REGEXP_CONTAINS(Table, r"^anon[0-9a-z]{40}") THEN 'anonymous'
        WHEN REGEXP_CONTAINS(Table, r"^anonev_[0-9a-zA-Z_]{40}") THEN 'anonymous'
        WHEN REGEXP_CONTAINS(Table, r"^_[0-9a-z]{40}") THEN 'temporary'
        ELSE Table
      END AS Table
    , IF(array_length(referencedTables) > 0, referencedTables[OFFSET(0)].datasetId, NULL) AS referencedDataset
    , IF(array_length(referencedTables) > 0, referencedTables[OFFSET(0)].tableId, NULL) AS referencedTable
  FROM billing_dataset
), billing_final AS (
  SELECT * EXCEPT(Dataset, Table, TotalCost)
    , IF(ARRAY_LENGTH(reservationUsage) = 0, TotalCost, 0) AS TotalCost
    , TotalCost AS RawTotalCost
    , ARRAY_LENGTH(reservationUsage) > 0 AS HasReservedSlots
    , IF((Dataset = 'temporary' AND Table = 'anonymous' AND NOT REGEXP_CONTAINS(referencedDataset, r"^anon[0-9a-z]{40}")), referencedDataset, Dataset) AS Dataset
    , IF((Dataset = 'temporary' AND Table = 'anonymous' AND NOT REGEXP_CONTAINS(referencedTable, r"^anon[0-9a-z]{40}")), referencedTable, Table) AS Table
    , Dataset AS DatasetNew
    , Table AS TableNew
  FROM billing_cleansed
)
SELECT *
FROM billing_final

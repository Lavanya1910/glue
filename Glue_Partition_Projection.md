# Glue Partition Projection

Question 12

Glue Partition Projection ▼
Scenario: Athena queries on 10 years of daily partitioned data (3,650+ partitions). Crawler adds partitions slowly. Improve query performance and partition discovery.

Solution:
Enable Glue partition projection in Athena table
Use date-based projection for automatic partition discovery
Eliminate crawler dependency with projection
Reduce Athena query planning time by 90%+
Configure projection with range/enum patterns
Maintain manual partitions for special cases
Real-World Example (Pinterest): Enabled partition projection reducing Athena query planning from 45sec to <2sec on 10TB+ datasets. Query cost reduced by 30%.
Partition Projection Configuration:
# Glue partition projection setup
import boto3
glue = boto3.client('glue')
athena = boto3.client('athena')
# Create table with partition projection (via Glue)
create_table_sql = """
CREATE EXTERNAL TABLE logs_partitioned_projected (
timestamp STRING,
level STRING,
 DataArchitectStud
message STRING,
service_name STRING,
error_code STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://bucket/logs/'
TBLPROPERTIES (
'projection.enabled' = 'true',
'projection.year.type' = 'integer',
'projection.year.range' = '2014,2025',
'projection.month.type' = 'integer',
'projection.month.range' = '1,12',
'projection.month.digits' = '2',
'projection.day.type' = 'integer',
'projection.day.range' = '1,31',
'projection.day.digits' = '2',
's3.partition.projection.enabled' = 'true',
'storage.location.template' =
's3://bucket/logs/year=${year}/month=${month}/day=${day}'
);
"""
# Execute DDL
response = athena.start_query_execution(
QueryString=create_table_sql,
QueryExecutionContext={'Database': 'analytics_db'},
ResultConfiguration={'OutputLocation': 's3://bucket/athena-results/'}
)
print(f"Query creation query: {response['QueryExecutionId']}")
# Alternative: Update table properties via Glue API
def enable_partition_projection(database_name, table_name):
# Get current table
table_response = glue.get_table(
DatabaseName=database_name,
Name=table_name
)
table_input = table_response['Table']
# Add projection properties
if 'Parameters' not in table_input:
table_input['Parameters'] = {}
table_input['Parameters'].update({
'projection.enabled': 'true',
'projection.year.type': 'integer',
'projection.year.range': '2020,2025',
 DataArchitectStud
'projection.month.type': 'integer',
'projection.month.range': '1,12',
'projection.month.digits': '2',
'projection.day.type': 'integer',
'projection.day.range': '1,31',
'projection.day.digits': '2',
's3.partition.projection.enabled': 'true',
'storage.location.template':
's3://bucket/logs/year=${year}/month=${month}/day=${day}'
})
# Update table
glue.update_table(
DatabaseName=database_name,
TableInput=table_input
)
print(f"Partition projection enabled for {database_name}.{table_name}") enable_partition_projection('analytics_db', 'logs_partitioned_projected')
# Query example using partition projection
query_with_projection = """
SELECT
service_name,
COUNT(*) as error_count,
COUNT(DISTINCT error_code) as unique_errors
FROM logs_partitioned_projected
WHERE year = 2025
AND month = 3
AND day BETWEEN 15 AND 20
GROUP BY service_name
ORDER BY error_count DESC;
"""
# Execute query - partition pruning happens automatically
response = athena.start_query_execution(
QueryString=query_with_projection,
QueryExecutionContext={'Database': 'analytics_db'},
ResultConfiguration={'OutputLocation': 's3://bucket/athena-results/'}
)
print(f"Query execution: {response['QueryExecutionId']}")
# Enum-based projection for service names
enum_projection_sql = """
CREATE EXTERNAL TABLE events_by_service (
event_id STRING,
timestamp STRING,
event_type STRING,
properties STRING
)
 DataArchitectStud
PARTITIONED BY (service_name STRING, date STRING)
STORED AS PARQUET
LOCATION 's3://bucket/events/'
TBLPROPERTIES (
'projection.enabled' = 'true',
'projection.service_name.type' = 'enum',
'projection.service_name.values' = 'web-api,mobile-app,batch-service,analytics- 'projection.date.type' = 'date',
'projection.date.range' = '2024-01-01,NOW',
'projection.date.format' = 'yyyy-MM-dd',
's3.partition.projection.enabled' = 'true',
'storage.location.template' =
's3://bucket/events/service=${service_name}/date=${date}'
);
"""
print("Partition projection configured")
Common Pitfalls:

Enabling projection without verifying partition path patterns
Using projection with non-uniform partition layouts
Not setting projection.enabled on S3 location
Missing digits padding for sequential partition keys
Projection Approach: Analyze partition structure → Choose projection type (date/enum) → Define ranges → Enable projection → Test coverage
Expert Tip: Use projection for time-series data with daily/hourly partitions. Verify projection covers all existing partitions. Combine with partition filtering in WHERE clauses for optimal performance. Monitor Athena query costs post-projection.

DataArchitectStudio - AWS Glue Interview Questions 
 DataArchitectStud
Master AWS Glue with Real-World Scenarios & Solutions 
Table of Contents 
1. Glue Job Performance Optimization 2. Glue Catalog & Metadata Management 3. Glue Connectors (Salesforce, SAP, Kafka) 4. Error Handling & Data Quality 5. Glue Streaming (real-time ETL) 6. Glue Cost Optimization 
7. Glue DataBrew (data preparation) 8. Glue Bookmarks (incremental processing) 9. Glue DPU Scaling strategies 10. Glue Triggers (scheduled vs event) 11. Glue Schema Registry 12. Glue Partition Projection 
Question 1 
Glue Job Performance Optimization (1TB, 2hrs→30min) ▼ 
Scenario: Your Glue job processes 1TB of data but takes 2 hours. Business requires 30-minute SLA. Current config: 10 DPU (G.2X), single partition processing. What optimization strategies would you implement? 

Solution: 
Increase DPU to 100+ G.2X for parallel execution 
Enable S3 intelligent tiering + CloudFront caching 
Implement partition pruning with Glue partitions 
Use coalesce() to reduce output file count 
Switch to Spark adaptive query execution 
Pre-aggregate data using Glue DataBrew 
 DataArchitectStud
Real-World Example (Netflix): Reduced data pipeline from 180min to 25min by migrating from EMR to Glue with 500 DPU allocation, S3 partitioning strategy, and DataFrame coalesce optimization. 
Python Implementation: 
# Glue Job for optimized 1TB processing 
from awsglue.context import GlueContext 
from awsglue.transforms import * 
from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("OptimizedGlueJob").getOrCreate() 
glueContext = GlueContext(spark.sparkContext) 
job = Job(glueContext) 
# Enable adaptive query execution 
spark.conf.set("spark.sql.adaptive.enabled", "true") 
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") 
# Read 1TB data with partition pruning 
df = spark.read.parquet("s3://bucket/data/year=2024/month=03/") 
# Filter early to reduce data volume 
filtered_df = df.filter(df.status == "active") 
# Apply transformations 
transformed_df = filtered_df.groupBy("customer_id").agg({"amount": "sum"}) 
# Coalesce to reduce shuffle overhead 
final_df = transformed_df.coalesce(50) 
# Write with partitioning 
final_df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/output/" 
job.commit() 
    Common Pitfalls: 

Blindly increasing DPU without fixing skewed joins 
Not considering S3 request throttling limits 
Using repartition() instead of coalesce() 
Ignoring Glue catalog stats for query optimization 
Performance Approach: Measure baseline → Profile with Spark UI → Identify shuffle/scan bottlenecks → 
Apply DPU scaling → Optimize I/O → Test SLA compliance 
 DataArchitectStud
Expert Tip: Use Glue job bookmarks with S3 intelligent tiering. Enable Spark AQE and monitor via CloudWatch metrics. Target 30-40 DPU per GB/sec throughput. 
Question 2 
Glue Catalog & Metadata Management ▼ 
Scenario: Managing 500+ tables across 10 databases. Need version control, lineage tracking, and tag based access control. How would you implement this? 
Solution: 
Use Glue Data Catalog with table versioning enabled 
Implement Lake Formation for fine-grained access control 
Add business/technical tags to all tables 
Enable Glue Data Quality rules for schema validation 
Use Lake Formation tag-based access policies 
Integrate with AWS Glue Lineage for impact analysis 
Real-World Example (Uber): Manages 2000+ tables using Glue Catalog with custom tagging taxonomy. Reduced metadata search time from 15min to <1sec with Athena federation and proper partitioning. 
Python Implementation: 
# Glue Catalog metadata management 
import boto3 
glue_client = boto3.client('glue') 
lf_client = boto3.client('lakeformation') 
# Create database with metadata 
database_input = { 
'Name': 'analytics_db', 

'Description': 'Production analytics database', 
'LocationUri': 's3://bucket/analytics/', 
'Parameters': { 
'owner': 'data-eng-team', 
'classification': 'analytics' 
} 
} 
glue_client.create_database(DatabaseInput=database_input) 
chitectStud
# Tag tables for access control 
glue_client.tag_resource( 
ResourceArn='arn:aws:glue:region:account:table/analytics_db/customers', TagsToAdd={ 
'environment': 'production', 
'classification': 'pii', 
'owner': 'analytics-team' 
} 
) 
# Apply Lake Formation tag-based access 
lf_client.put_data_lake_settings( 
DataLakeSettings={ 
'DataLakeAdmins': [ 
{'DataLakePrincipalIdentifier': 'arn:aws:iam::account:role/GlueServiceR ] 
} 
) 
 DataAr 
# Attach tag-based policy 
lf_client.grant_permissions( 
Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::account:role/AnalyticsR Permissions=['DESCRIBE', 'SELECT'], 
PermissionsWithGrantOption=['DESCRIBE'], 
Resource={ 
'DataLocationResource': { 
'Arn': 's3://bucket/analytics/' 
} 
} 
) 
print("Catalog and access control configured") 
Common Pitfalls: 
Not using partitioning for large catalogs (>1000 tables) 
Storing sensitive metadata in table descriptions 
Missing schema evolution tracking 
No automated cleanup of stale table versions 

Metadata Approach: Design tagging taxonomy → Implement Lake Formation → Enable version tracking → Set retention policies → Automate validation 
Expert Tip: Use consistent naming conventions (env_domain_entity). Tag during table creation via automation. Query Glue Catalog API for metadata-driven pipelines. 
 DataArchitectStud
Question 3 
Glue Connectors (Salesforce, SAP, Kafka) ▼ 
Scenario: Integrate Salesforce, SAP, and Kafka into Glue data lake. Handling authentication, incremental sync, and schema changes. Implementation approach? 
Solution: 
Use AWS Glue connectors from marketplace 
Store credentials in AWS Secrets Manager 
Implement incremental extraction with timestamps 
Handle schema drift with Glue schema registry 
Use Glue job bookmarks for Kafka offset tracking 
Partition output by extraction date/time 
Real-World Example (Salesforce at Scale): Synced 50M Salesforce records daily using Glue connectors with change data capture. Reduced manual ETL from 8hrs to 45min automation. 
Python Implementation: 
# Multi-connector Glue job 
from awsglue.context import GlueContext 
from awsglue.job import Job 
import boto3 
import json 
glueContext = GlueContext(spark.sparkContext) 
job = Job(glueContext) 
secrets_client = boto3.client('secretsmanager') 
# Retrieve credentials from Secrets Manager 
def get_secret(secret_name): 
response = secrets_client.get_secret_value(SecretId=secret_name) 
return json.loads(response['SecretString']) 
# Salesforce connector with incremental sync 
sf_creds = get_secret('salesforce/prod/api') 
sf_df = glueContext.create_dynamic_frame.from_options( 
connection_type="salesforce", 
format_options={ 
"salesforce_instance": sf_creds['instance'], 
"username": sf_creds['username'], 
"password": sf_creds['password'], 
 DataArchitectStud
"security_token": sf_creds['token'], 
"object": "Account" 
} 
).toDF() 
# Kafka connector with offset management 
kafka_df = spark.readStream \ 
.format("kafka") \ 
.option("kafka.bootstrap.servers", "broker:9092") \ 
.option("subscribe", "orders-topic") \ 
.option("startingOffsets", "earliest") \ 
.load() 
# SAP connector 
sap_creds = get_secret('sap/prod/credentials') 
sap_df = glueContext.create_dynamic_frame.from_options( 
connection_type="sap", 
format_options={ 
"host": sap_creds['host'], 
"port": sap_creds['port'], 
"client": sap_creds['client'], 
"table": "VBAK" 
} 
).toDF() 
# Merge and write 
merged_df = sf_df.union(sap_df) 
merged_df.write.mode("overwrite").partitionBy("extraction_date").parquet("s3://buck job.commit() 
Common Pitfalls: 
Storing credentials in code or job parameters 
Full reload instead of incremental sync 
Not handling API rate limits 
Missing error handling for transient failures 
Connector Approach: Validate credentials → Test connectivity → Implement incremental logic → Handle schema changes → Monitor API quotas 
Expert Tip: Always use Secrets Manager rotation. Implement exponential backoff for retries. Use Glue Connector SDK for custom connectors. Test with small datasets first. 
Question 4 
 DataArchitectStud
Error Handling & Data Quality ▼ 
Scenario: Pipeline processes financial transactions. Need robust error handling, data quality checks, and alerting for anomalies. How would you implement? 
Solution: 
Implement Glue Data Quality rules framework 
Add schema validation with Glue schema registry 
Use circuit breaker pattern for external calls 
Store bad records in quarantine bucket 
Set up CloudWatch alarms for quality metrics 
Implement automated remediation workflows 
Real-World Example (JPMorgan Chase): Implemented Glue data quality checks on $1T daily transaction volume. Caught 99.2% of data quality issues before downstream processing. 
Python Implementation: 
# Data quality and error handling 
from awsglue.context import GlueContext 
from awsglue.job import Job 
from awsglue.quality import DataQualityRulesBuilder 
import boto3 
glueContext = GlueContext(spark.sparkContext) 
cloudwatch = boto3.client('cloudwatch') 
# Define data quality rules 
dqr = DataQualityRulesBuilder().append_rule( 
"ColumnExists=amount" 
).append_rule( 
"ColumnValuesMatching=amount,[0-9]+.[0-9]{2}" 
).append_rule( 
"ColumnLength=transaction_id,32" 
).build() 

# Read with error handling 
try: 
df = spark.read.parquet("s3://bucket/transactions/") 
# Apply data quality rules 
dq_results = glueContext.data_quality.evaluate_with_rules(df, dqr) # Separate good and bad records 

if dq_results.success: 
chitectStud

good_records = df.filter(df.amount > 0) 
bad_records = df.filter(df.amount <= 0) 
# Store bad records for review 
bad_records.write.mode("append").parquet("s3://bucket/quarantine/") 
# Process good records 
processed = good_records.groupBy("account_id").agg({"amount": "sum"}) processed.write.mode("overwrite").parquet("s3://bucket/output/") 
# Publish metrics 
cloudwatch.put_metric_data( 
Namespace='DataQuality', 
MetricData=[ 
{ 
'MetricName': 'SuccessfulRecords', 
'Value': good_records.count(), 
'Unit': 'Count' 
}, 
 DataAr 
{ 
'MetricName': 'FailedRecords', 
'Value': bad_records.count(), 
'Unit': 'Count' 
} 
] 
) 
except Exception as e: 
# Log and alert 
cloudwatch.put_metric_data( 
Namespace='DataQuality', 
MetricData=[ 
{ 
'MetricName': 'JobFailures', 
'Value': 1, 
'Unit': 'Count' 
} 
] 
) 
raise 
Common Pitfalls: 
Failing entire job for single bad record 
Not implementing retry logic for transient failures 
Ignoring null/empty value validation 
Missing data freshness checks 
 DataArchitectStud
Quality Approach: Define quality rules → Validate early → Quarantine bad data → Alert on anomalies → Track metrics → Automate fixes 
Expert Tip: Use Glue Data Quality rules with AWS Glue native integration. Implement SLA monitoring. Keep quarantine bucket organized by error type and timestamp. 
Question 5 
Glue Streaming (real-time ETL) ▼ 
Scenario: Real-time stream of IoT sensor data from 10,000 devices. Need to process 100K events/second, perform aggregations, detect anomalies. Architecture and implementation? 
Solution: 
Use Glue Streaming with Kafka/Kinesis source 
Implement microbatch processing (1-5 second windows) 
Use Spark structured streaming for stateful operations 
Apply watermarking for late-arriving data 
Write results to S3 + DynamoDB for fast lookup 
Monitor checkpoint health and lag 
Real-World Example (Tesla): Processes 2M+ telemetry events/sec using Glue Streaming. Detects vehicle anomalies within 50ms and triggers alerts to service centers. 
Python Implementation: 
# Glue Streaming for IoT data 
from awsglue.context import GlueContext 
from pyspark.sql.functions import from_json, col, window, avg 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType glueContext = GlueContext(spark.sparkContext) 

# Define schema 
schema = StructType([ 
StructField("device_id", StringType()), StructField("temperature", DoubleType()), StructField("humidity", DoubleType()), StructField("timestamp", StringType()) ]) 

# Read from Kinesis stream df = spark.readStream \ .format("kinesis") \ 
chitectStud

.option("streamName", "iot-sensors") \ 
.option("region", "us-east-1") \ 
.option("initialPosition", "TRIM_HORIZON") \ 
.load() 
# Parse JSON 
parsed_df = df.select( 
from_json(col("data").cast("string"), schema).alias("parsed") ).select("parsed.*") 
# Time-window aggregation 
windowed = parsed_df \ 
.withWatermark("timestamp", "10 seconds") \ 
.groupBy(window("timestamp", "5 seconds"), "device_id") \ 
.agg( 
avg("temperature").alias("avg_temp"), 
avg("humidity").alias("avg_humidity") 
 DataAr 
) 
# Anomaly detection 
anomalies = windowed.filter(col("avg_temp") > 50) 
# Write to S3 (parquet) 
query1 = parsed_df.writeStream \ 
.format("parquet") \ 
.option("path", "s3://bucket/iot-data/") \ 
.option("checkpointLocation", "s3://bucket/checkpoints/iot/") \ .option("mergeSchema", "true") \ 
.start() 
# Write anomalies to DynamoDB for real-time alerting 
query2 = anomalies.writeStream \ 
.format("dynamodb") \ 
.option("table", "IoTAnomalies") \ 
.option("region", "us-east-1") \ 
.option("checkpointLocation", "s3://bucket/checkpoints/anomalies/") \ .start() 
query1.awaitTermination() 
query2.awaitTermination() 
Common Pitfalls: 
Using batch mode for streaming workloads 
Not managing state size for stateful operations 
Ignoring late-arriving data (no watermarking) 
High checkpoint latency causing processing lag 
chitectStud
Streaming Approach: Define schema → Set microbatch interval → Implement stateful ops → Add watermarking → Monitor lag metrics 
Expert Tip: Use 5-second windows for balance between latency and overhead. Monitor checkpoint duration and state size. Partition output by device_id for efficient querying. 
Question 6 
Glue Cost Optimization ▼ 
Scenario: Monthly Glue bill is $50K with 100 running jobs. Target: reduce to $20K without impacting SLA.  DataAr 
Cost optimization strategy? 
Solution: 
Right-size DPU allocation per job (audit current usage) 
Consolidate 10-15 small jobs into single job 
Use G.1X (cheaper) instead of G.2X where I/O limited 
Implement job failure early exits to avoid wasted DPU 
Schedule non-critical jobs during off-peak hours 
Cache intermediate results in S3 to avoid recomputation 
Real-World Example (Airbnb): Reduced Glue costs by 60% by migrating 300 jobs to G.1X and implementing job consolidation. Maintained same SLA. 
Cost Analysis & Optimization: 
# Cost optimization analysis 
import boto3 
import json 
from datetime import datetime, timedelta 

glue_client = boto3.client('glue') 
ce_client = boto3.client('ce') 
# Get all jobs with metrics 
def analyze_job_costs(): 
jobs = glue_client.list_jobs()['JobNames'] 
for job_name in jobs: 
job_detail = glue_client.get_job(Name=job_name)['Job'] 
 DataArchitectStud
# Extract metrics 
max_capacity = job_detail.get('MaxCapacity', 0) 
glue_version = job_detail.get('GlueVersion', '3.0') 
worker_type = job_detail.get('WorkerType', 'G.2X') 
# Monthly cost calculation 
# G.2X: $0.44/DPU-hour, G.1X: $0.25/DPU-hour 
hourly_cost_g2x = max_capacity * 0.44 
hourly_cost_g1x = max_capacity * 0.25 
monthly_cost = hourly_cost_g2x * 168 # 730 hours/month 
print(f"Job: {job_name}") 
print(f" Current: {worker_type}, {max_capacity} DPU -> ${monthly_cost:.2f print(f" G.1X savings: ${(hourly_cost_g2x - hourly_cost_g1x) * 730:.2f}/mo print() 
# Query Cost Explorer for Glue spending 
def get_glue_cost_breakdown(): 
response = ce_client.get_cost_and_usage( 
TimePeriod={ 
'Start': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'), 
'End': datetime.now().strftime('%Y-%m-%d') 
}, 
Granularity='DAILY', 
Filter={'Dimensions': {'Key': 'SERVICE', 'Values': ['AWS Glue']}}, 
Metrics=['UnblendedCost'] 
) 
total_cost = sum(float(day['Total']['UnblendedCost']['Amount']) 
for day in response['ResultsByTime']) 
print(f"30-day Glue spending: ${total_cost:.2f}") 
return total_cost 
analyze_job_costs() 
get_glue_cost_breakdown() 
Common Pitfalls: 
Allocating max DPU to all jobs without profiling 

Running duplicate jobs unintentionally 
Not using reserved capacity discounts 
Ignoring S3 request costs in optimization 
Cost Approach: Audit current usage → Profile job requirements → Right-size DPU → Consolidate jobs → Monitor Cost Explorer 
 DataArchitectStud
Expert Tip: Use AWS Compute Optimizer for DPU recommendations. Analyze CloudWatch metrics before scaling. G.1X is sufficient for most IO-bound workloads. Consider Glue reserved capacity for predictable workloads. 
Question 7 
Glue DataBrew (data preparation) ▼ 
Scenario: Customer dataset with 40% missing values, inconsistent date formats, duplicate records. Need quick data cleaning. DataBrew approach? 
Solution: 
Use DataBrew for visual data profiling and cleaning 
Create recipes for standardizing formats (dates, phone numbers) 
Implement missing value handling rules 
Remove duplicate rows using DataBrew deduplication 
Create data quality checks as part of recipe 
Schedule DataBrew job runs as part of pipeline 
Real-World Example (Capital One): Cleaned 500M customer records using DataBrew recipes. Reduced manual data cleaning time from 40% of ETL to 5%. 
DataBrew Recipe and Job Setup: 
# AWS Glue DataBrew setup via SDK 
import boto3 
import json 
databrew = boto3.client('databrew') 
# Create dataset from S3 
dataset_response = databrew.create_dataset( 
Name='customer-raw-data', 

Input={ 
'S3InputFormatOptions': { 
'Json': {'MultiLine': False} 
}, 
'DatasetInputFormat': 'PARQUET', 
'S3InputFormatOptions': {} 
}, 
Format='PARQUET', 
FormatOptions={ 
 DataArchitectStud
'Json': {'MultiLine': False} 
}, 
Paths=['s3://bucket/raw-data/customers/'] 
) 
# Create cleaning recipe 
recipe_response = databrew.create_recipe( 
Name='customer-cleaning-recipe', 
Steps=[ 
{ 
'Action': { 
'Operation': 'DELETE_DUPLICATES_ROW', 
'Parameters': {} 
} 
}, 
{ 
'Action': { 
'Operation': 'FORMAT_CELL_VALUES', 
'Parameters': { 
'sourceColumn': 'phone_number', 
'targetDateFormat': '(###) ###-####', 
'targetType': 'STRING' 
} 
} 
}, 
{ 
'Action': { 
'Operation': 'FILL_DOWN', 
'Parameters': { 
'columnToFill': 'missing_email' 
} 
} 
}, 
{ 
'Action': { 
'Operation': 'STANDARDIZE_DATE_FORMAT', 
'Parameters': { 
'sourceColumn': 'date_of_birth', 
'sourceFormat': 'YYYY-MM-DD', 
'targetFormat': 'MM/DD/YYYY' 
} 
} 
} 
] 
) 
# Create and run job 
job_response = databrew.create_job( Name='customer-cleanup-job', Type='PROFILE', 
DatasetName='customer-raw-data', 

RecipeReference={ 
chitectStud

'Name': 'customer-cleaning-recipe' 
}, 
Outputs=[ 
{ 
'Location': { 
'Bucket': 'bucket', 
'Key': 'cleaned-data/customers/' 
}, 
'Format': 'PARQUET' 
} 
], 
RoleArn='arn:aws:iam::account:role/DataBrewRole', 
MaxCapacity=5, 
Timeout=300 
) 
# Run the job 
run_response = databrew.start_job_run(Name='customer-cleanup-job') 
print(f"Job run started: {run_response['RunId']}") 
 DataAr 
Common Pitfalls: 
Using DataBrew for large transformations (use Glue for complex logic) 
Not profiling data before creating recipes 
Ignoring case sensitivity in string operations 
Missing data validation after cleaning 
DataBrew Approach: Profile data → Identify issues → Build recipes → Test recipes → Schedule jobs → Validate output 
Expert Tip: Use DataBrew for <5GB datasets. Profile first to understand data patterns. Export recipes as JSON for version control. Combine with Glue jobs for complex transformations. 
Question 8 
Glue Bookmarks (incremental processing) ▼ 
Scenario: Process 1M+ new database records daily. Current full reload takes 4 hours. Need incremental processing with bookmark-based tracking. Implementation? 
 DataArchitectStud
Solution: 
Enable Glue bookmarks on source connector 
Use LastModified timestamp column for CDC 
Configure bookmark update strategy (success/fail) 
Handle bookmarks for S3 sources with prefix keys 
Monitor bookmark state in CloudWatch 
Implement manual bookmark reset for recovery 
Real-World Example (Amazon Retail): Processes 500M product updates daily using bookmarks. Reduced processing time from 240min to 15min with incremental sync. 
Python Implementation: 
# Glue bookmarks for incremental processing 
from awsglue.context import GlueContext 
from awsglue.transforms import ApplyMapping 
from awsglue.job import Job 
from awsglue.dynamicframe import DynamicFrame 
import boto3 
glueContext = GlueContext(spark.sparkContext) 
job = Job(glueContext) 
# Initialize job with bookmark support 
job.init({ 
'TempDir': 's3://bucket/temp/', 
'JobName': 'IncrementalSyncJob' 
}) 
# Read from JDBC with bookmark 
datasource = glueContext.create_dynamic_frame.from_options( 
connection_type="postgresql", 
connection_options={ 
"url": "jdbc:postgresql://db.rds.amazonaws.com:5432/prod_db", 
"dbtable": "customers", 
"user": "admin", 
"password": "password123", 
"customFilters": "last_modified > {bookmark}", 
"bookmarkColumn": "last_modified" 
}, 
transformation_ctx="CustomersDynamicFrame" 

) 
# Convert to DataFrame for transformations 
df = datasource.toDF() 
# Apply transformations 
transformed_df = df.select( 
"customer_id", 
"name", 
 DataArchitectStud
"email", 
"last_modified" 
) 
# Write output with partitioning 
output_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "OutputDYF") glueContext.write_dynamic_frame.from_options( 
frame=output_dyf, 
connection_type="s3", 
format="parquet", 
connection_options={ 
"path": "s3://bucket/incremental-data/", 
"partitionKeys": ["extraction_date"] 
}, 
transformation_ctx="WriteOutput" 
) 
job.commit() 
# Monitor bookmark state 
glue = boto3.client('glue') 
job_runs = glue.get_job_runs(JobName='IncrementalSyncJob') 
for run in job_runs['JobRuns'][:5]: 
print(f"Run ID: {run['Id']}") 
print(f"Status: {run['JobRunState']}") 
if 'BookmarkLocation' in run: 
print(f"Bookmark: {run['BookmarkLocation']}") 
print() 
Common Pitfalls: 
Using bookmarks with non-monotonic timestamp columns 
Not resetting bookmarks after data corrections 
Ignoring out-of-order record arrivals 
Missing deleted record handling (soft deletes) 
Bookmark Approach: Enable bookmarks → Add monotonic timestamp → Test incremental logic → Monitor state → Plan recovery strategy 

Expert Tip: Always use LastModified or sequence number columns for bookmarks. Implement CDC (Change Data Capture) pattern for deletions. Test bookmark reset scenarios before production. 
Question 9 
 DataArchitectStud
Glue DPU Scaling strategies ▼ 
Scenario: Glue workloads vary: small 10DPU jobs peak at 2PM, medium 50DPU jobs at 5PM, large 200DPU batch at 10PM. Design optimal DPU allocation strategy. 
Solution: 
Profile jobs to find minimum viable DPU 
Use auto-scaling for streaming jobs 
Schedule jobs with staggered start times 
Reserve capacity for critical workloads 
Implement job chaining to reduce concurrency 
Monitor DPU utilization via CloudWatch 
Real-World Example (LinkedIn): Implemented dynamic DPU scaling reducing peak capacity needs by 40% while maintaining SLA through intelligent job scheduling. 
DPU Scaling Strategy: 
# Intelligent DPU scaling for Glue jobs 
import boto3 
from datetime import datetime, timedelta 
glue = boto3.client('glue') 
cloudwatch = boto3.client('cloudwatch') 
# Get job execution patterns 
def analyze_dpu_patterns(): 
jobs = glue.list_jobs()['JobNames'] 
job_schedule = { 
'low_volume_jobs': { 
'jobs': ['data_validation', 'metadata_sync'], 
'dpu': 5, 
'schedule': '0 1 * * *' # 1 AM - off-peak 
}, 
'medium_jobs': { 

'jobs': ['customer_etl', 'product_sync'], 
'dpu': 25, 
'schedule': '0 8 * * *' # 8 AM - low-peak 
}, 
'large_batch_jobs': { 
'jobs': ['full_warehouse_sync', 'historical_backfill'], 
'dpu': 100, 
'schedule': '0 22 * * *' # 10 PM - night batch 
} 
 DataArchitectStud
} 
return job_schedule 
# Update job with recommended DPU 
def optimize_job_dpu(job_name, target_dpu): 
glue.update_job( 
Name=job_name, 
JobUpdate={ 
'MaxCapacity': target_dpu, 
'GlueVersion': '4.0', 
'WorkerType': 'G.2X' # Auto-select based on DPU 
} 
) 
print(f"Updated {job_name} to {target_dpu} DPU") 
# Get DPU utilization metrics 
def get_dpu_utilization(job_name, hours=24): 
response = cloudwatch.get_metric_statistics( 
Namespace='AWS/Glue', 
MetricName='dpu_hour_usage', 
Dimensions=[{'Name': 'JobName', 'Value': job_name}], 
StartTime=datetime.now() - timedelta(hours=hours), 
EndTime=datetime.now(), 
Period=3600, 
Statistics=['Average', 'Maximum'] 
) 
datapoints = response['Datapoints'] 
if datapoints: 
avg_utilization = sum(d['Average'] for d in datapoints) / len(datapoints) max_utilization = max(d['Maximum'] for d in datapoints) 
print(f"{job_name}: Avg {avg_utilization:.1f}, Max {max_utilization:.1f} DP return datapoints 
# Recommendation engine 
def recommend_dpu(job_name): 
metrics = get_dpu_utilization(job_name) 
if metrics: 
max_util = max(d['Maximum'] for d in metrics) 
if max_util < 10: 
return 5 
elif max_util < 50: 
return 25 
else: 
return max(100, int(max_util * 1.2)) # 20% buffer 
return 10 # default 
# Apply recommendations 
 DataArchitectStud
schedule = analyze_dpu_patterns() 
for tier, config in schedule.items(): 
for job in config['jobs']: 
recommended_dpu = recommend_dpu(job) 
optimize job dpu(job, recommended dpu) 
Common Pitfalls: 
Over-allocating DPU based on peak (not average) usage 
Not accounting for Spark shuffle overhead 
Ignoring job dependencies causing contention 
Not monitoring actual DPU utilization metrics 
Scaling Approach: Profile baseline → Find minimum DPU → Monitor utilization → Implement scheduling → Reserve capacity → Test peaks 
Expert Tip: Use 30% DPU utilization target for bursty workloads. Implement job chaining to reduce peak concurrency. Monitor shuffle metrics to identify over-allocation. 
Question 10 
Glue Triggers (scheduled vs event) ▼ 
Scenario: Complex pipeline: Import → Clean → Aggregate → Report. Import can be manual or automated. Clean depends on import completion. Design trigger strategy for reliability. 
Solution: 
Use scheduled trigger for regular imports (cron) 
Use conditional trigger for dependent jobs 
Implement timeout and retry logic 
Set up failure notifications via SNS 

Create monitoring dashboard for trigger execution 
Implement manual trigger capability for ad-hoc runs 
Real-World Example (Spotify): Manages 500+ interdependent Glue jobs using triggers. Automatic recovery on failures reduced manual intervention from 30% to 2% of pipeline runs. 
 DataArchitectStud
Trigger Configuration: 
# Glue trigger setup for complex pipelines 
import boto3 
import json 
glue = boto3.client('glue') 
sns = boto3.client('sns') 
# Create scheduled trigger for import job 
scheduled_trigger = { 
'Name': 'ImportDataDaily', 
'Description': 'Trigger import job daily at 2 AM', 
'Type': 'SCHEDULED', 
'Schedule': 'cron(0 2 * * ? *)', 
'Actions': [ 
{ 
'JobName': 'ImportJob', 
'Arguments': { 
'--source': 'sftp://external-server/data/', 
'--destination': 's3://bucket/imports/' 
}, 
'Timeout': 300, 
'NotificationProperty': { 
'NotifyDelayAfter': 600 
} 
} 
], 
'StartOnCreation': True 
} 
glue.create_trigger(**scheduled_trigger) 
# Create conditional trigger for downstream jobs 
conditional_trigger = { 
'Name': 'RunCleanupOnImportSuccess', 
'Description': 'Run cleanup after successful import', 
'Type': 'CONDITIONAL', 
'Actions': [ 
{ 
'JobName': 'CleanupJob', 
'Arguments': { 
'--input': 's3://bucket/imports/', 
'--output': 's3://bucket/cleaned/' 

}, 
'Timeout': 600, 
'NotificationProperty': { 
'NotifyDelayAfter': 900 
} 
} 
], 
'Predicate': { 
'Logical': 'ANY', 
 DataArchitectStud
'Conditions': [ 
{ 
'LogicalOperator': 'EQUALS', 
'CrawlerName': 'ImportDataCrawler', 
'State': 'SUCCEEDED' 
} 
] 
}, 
'StartOnCreation': True 
} 
glue.create_trigger(**conditional_trigger) 
# Create SNS notification for failures 
def setup_failure_notifications(): 
topic = sns.create_topic(Name='GluePipelineFailures') 
topic_arn = topic['TopicArn'] 
# Subscribe to notifications 
sns.subscribe( 
TopicArn=topic_arn, 
Protocol='email', 
Endpoint='data-eng-team@company.com' 
) 
return topic_arn 
# Event-driven trigger (on S3 upload) 
event_trigger = { 
'Name': 'ProcessOnS3Upload', 
'Description': 'Process when new file uploaded to input bucket', 
'Type': 'ON_DEMAND', # Manual + S3 events via EventBridge 
'Actions': [ 
{ 
'JobName': 'ProcessUploadedFile', 
'Arguments': { 
'--input-bucket': 's3://uploads/' 
} 
} 
] 
} 
glue.create_trigger(**event_trigger) 

print("Triggers configured successfully") 
Common Pitfalls: 
Circular dependencies between triggers 
 DataArchitectStud
Not implementing retry logic in triggers 
Ignoring timeout configuration (jobs killed prematurely) 
Missing alerting for trigger failures 
Trigger Approach: Map job dependencies → Choose trigger types → Implement error handling → Set notifications → Test failure scenarios 
Expert Tip: Use CONDITIONAL triggers for sequential pipelines. Implement exponential backoff in retry logic. Add 10-minute delays for data consistency. Use EventBridge for complex event-driven workflows. 
Question 11 
Glue Schema Registry ▼ 
Scenario: 50+ teams producing events to Kafka. Need schema governance, compatibility checks, versioning. Schema Registry implementation? 
Solution: 
Create Glue Schema Registry for event schemas 
Define compatibility rules (BACKWARD, FORWARD, FULL) 
Implement schema versioning and evolution 
Add serializers/deserializers for Kafka integration 
Enforce schema validation in producers 
Set up compliance checks and auditing 
Real-World Example (Netflix): Manages 10K+ event schemas using Glue Schema Registry. Prevented 99% of schema-related production issues through governance. 
Schema Registry Setup: 
# Glue Schema Registry for event governance 
import boto3 

import json 
import avro.schema 
from kafka import KafkaProducer 
glue = boto3.client('glue') 
# Create Schema Registry 
registry_name = 'EventRegistry' 
glue.create_registry( 
 DataArchitectStud
RegistryName=registry_name, 
Description='Central event schema governance' 
) 
# Define Avro schema for Order event 
order_schema = { 
"type": "record", 
"name": "Order", 
"namespace": "com.company.events", 
"fields": [ 
{"name": "order_id", "type": "string"}, 
{"name": "customer_id", "type": "string"}, 
{"name": "amount", "type": "double"}, 
{"name": "currency", "type": "string", "default": "USD"}, 
{"name": "timestamp", "type": "long"} 
] 
} 
# Register schema with versioning 
schema_response = glue.create_schema( 
RegistryId={'RegistryName': registry_name}, 
SchemaName='OrderEvent', 
DataFormat='AVRO', 
Compatibility='BACKWARD', # Allows reading old data with new schema 
SchemaDefinition=json.dumps(order_schema), 
Tags={'team': 'order-service', 'domain': 'commerce'} 
) 
schema_version_id = schema_response['SchemaVersionId'] 
print(f"Schema registered: {schema_version_id}") 
# Schema evolution - adding optional field 
evolved_schema = { 
"type": "record", 
"name": "Order", 
"namespace": "com.company.events", 
"fields": [ 
{"name": "order_id", "type": "string"}, 
{"name": "customer_id", "type": "string"}, 
{"name": "amount", "type": "double"}, 
{"name": "currency", "type": "string", "default": "USD"}, 
{"name": "timestamp", "type": "long"}, 
{"name": "region", "type": "string", "default": "US"} # New field 
] 
} 
# Check compatibility before registering 
compatibility_check = glue.check_schema_version_validity( 
SchemaVersionId={'SchemaArn': schema_response['SchemaArn']}, 
DataFormat='AVRO', 
SchemaDefinition=json.dumps(evolved_schema) 
) 
 DataArchitectStud
if compatibility_check['Valid']: 
# Register evolved schema 
evolved_version = glue.put_schema_version_metadata( 
SchemaVersionId={'SchemaArn': schema_response['SchemaArn']}, 
SchemaVersionMetadata=[ 
{ 
'MetadataKey': 'version', 
'MetadataValue': '1.1' 
} 
] 
) 
print("Schema evolved successfully") 
# Use with Kafka producer 
def create_kafka_producer_with_schema_registry(): 
producer = KafkaProducer( 
bootstrap_servers=['kafka:9092'], 
value_serializer=lambda v: json.dumps(v).encode('utf-8') 
) 
# Validate event before sending 
def send_order_event(order_event): 
# Validate against schema 
schema = avro.schema.parse(json.dumps(order_schema)) 
try: 
avro.io.validate(schema, order_event) 
producer.send('orders', value=order_event) 
print(f"Order {order_event['order_id']} sent") 
except Exception as e: 
print(f"Schema validation failed: {e}") 
return send_order_event 
# List all schemas in registry 
def list_all_schemas(): 
schemas = glue.list_schemas(RegistryId={'RegistryName': registry_name}) 
for schema in schemas['Schemas']: 
print(f"Schema: {schema['SchemaName']}") 
print(f" Latest Version: {schema['LatestSchemaVersion']}") 
print(f" Compatibility: {schema['Compatibility']}") 
print() 

list_all_schemas() 
Common Pitfalls: 
Using NONE compatibility allowing breaking changes 
 DataArchitectStud
Not documenting schema changes in registry 
Missing schema version in Kafka message headers 
Ignoring field ordering in Avro schemas 
Registry Approach: Design schema taxonomy → Set compatibility rules → Implement validation → Version schemas → Track lineage 
Expert Tip: Use BACKWARD compatibility for schema evolution. Always include schema version in message headers. Document all schema changes with justification. Use namespace to avoid conflicts. 
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
print(f"Table creation query: {response['QueryExecutionId']}") 
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
© DataArchitectStudio - AWS Glue Interview Questions 
Follow @DataArchitectStudio | Master AWS Data Engineering 
 DataArchitectStud

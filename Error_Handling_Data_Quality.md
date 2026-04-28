# Error Handling & Data Quality

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

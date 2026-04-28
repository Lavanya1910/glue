# Glue Connectors (Salesforce, SAP, Kafka)

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

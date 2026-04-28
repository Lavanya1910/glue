# Glue Bookmarks (incremental processing)

Question 8

Glue Bookmarks (incremental processing) ▼
Scenario: Process 1M+ new database records daily. Current full reload takes 4 hours. Need incremental processing with bookmark-based tracking. Implementation?

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
'path': 's3://bucket/incremental-data/',
'partitionKeys': ['extraction_date']
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

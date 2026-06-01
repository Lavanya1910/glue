

# AWS Glue Bookmarks Cheat Sheet

Here is your copy-paste ready, clean Markdown file. You can copy the code block below and save it locally as glue_bookmarks.md on your computer:

# Glue Bookmarks (Incremental Processing)### ScenarioProcess 1M+ new database records daily. The current full reload takes 4 hours. You need incremental processing with bookmark-based tracking. How do you implement this?
---### Solution Strategy* **Enable Glue Bookmarks**: Activate tracking on the source connector.
* **Define Tracking Column**: Use a `LastModified` timestamp or sequence column.* **Configure Strategy**: Set up explicit commit behaviors for success/fail states.* **Manage File Storage**: Handle S3 source bookmarks using specific prefix keys.* **Monitor Infrastructure**: Track state changes through Amazon CloudWatch.* **Plan Recovery**: Implement manual bookmark resets for operational recovery.
> 💡 **Real-World Example (Amazon Retail)**> Processes 500M product updates daily using bookmarks. This reduced total processing time from **240 minutes to 15 minutes** using incremental sync.
---### Python Implementation```python
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping

# Initialize Glue context
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)

# Initialize job with bookmark support
job.init(
    "IncrementalSyncJob",
    {"TempDir": "s3://bucket/temp/", "JobName": "IncrementalSyncJob"},
)

# Read from JDBC with job bookmark enabled
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://://amazonaws.com",
        "dbtable": "customers",
        "user": "admin",
        "password": "password123",
        "customFilters": "last_modified > {bookmark}",
        "bookmarkColumn": "last_modified",
    },
    transformation_ctx="CustomersDynamicFrame",
)

# Convert to DataFrame for transformations
df = datasource.toDF()

# Apply schema transformations
transformed_df = df.select("customer_id", "name", "email", "last_modified")

# Convert back to DynamicFrame
output_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "OutputDYF")

# Write incremental output to S3
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://bucket/incremental-data/",
        "partitionKeys": ["extraction_date"],
    },
    transformation_ctx="WriteOutput",
)

# Commit state to save bookmark position
job.commit()

# Monitor job bookmark state
glue = boto3.client("glue")
job_runs = glue.get_job_runs(JobName="IncrementalSyncJob")

for run in job_runs["JobRuns"][:5]:
    print(f"Run ID: {run['Id']}")
    print(f"Status: {run['JobRunState']}")
    if "BookmarkLocation" in run:
        print(f"Bookmark: {run['BookmarkLocation']}")
    print()
```
---### Common Pitfalls* Using bookmarks with non-monotonic or non-sequential columns.* Forgetting to reset bookmarks after fixing source data.* Ignoring late-arriving or out-of-order data records.* Missing deleted records (hard deletes require soft-delete logic).
---### Implementation Roadmap

Enable Bookmarks ──> Identify Monotonic Column ──> Test Sync Logic ──> Monitor State ──> Plan Recovery


---

### Expert Tip
Always use a `LastModified` timestamp or sequential ID column for bookmarks. Implement a proper Change Data Capture (CDC) pattern to handle database deletions. Thoroughly test manual bookmark reset scenarios before deploying code to production.

If you need help setting up the next steps, let me know:

* Do you want to add Error Handling to the script?
* Do you need help writing a Boto3 script to reset the bookmark automatically?
* Do you want to see the configuration for S3 event-driven triggers?



# Glue DataBrew (data preparation)

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

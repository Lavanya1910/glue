# Glue Triggers (scheduled vs event)

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

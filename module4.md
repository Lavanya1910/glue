# MODULE 4: ORCHESTRATION & WORKFLOWS


## Q11. How would you orchestrate three Glue jobs (Clean → Aggregate → Load) so they run in
order automatically? 
I would use a Glue Workflow with conditional triggers so the jobs run in sequence and only
proceed on success.
Step-by-step:
•  Create one Glue Workflow to represent the full pipeline.
•  Add three Glue jobs: Clean, Aggregate, and Load.
•  Add a Start trigger that launches the Clean job when the workflow starts.
•  Add a conditional trigger for Aggregate with “on success of Clean”.
•  Add a conditional trigger for Load with “on success of Aggregate”.
•  Pass runtime parameters through the workflow (for example, processing date, S3
input/output paths, and run ID). I set these as workflow run properties and map them to
each job’s arguments.
•  Set maximum concurrency to 1 within the workflow path so only the intended next step
runs.
•  Make each job idempotent:
•  Clean writes to a dated/atomic output path (e.g., s3://…/clean/dt=YYYY-MMDD/),
then does a success marker file only after write completes.
•  Aggregate reads only the success-marked partition from Clean and writes its
own success marker.
•  Load reads only success-marked Aggregate output and writes in an idempotent
way to the target (e.g., MERGE into Redshift/Iceberg table or overwrite partition).
•  Add monitoring and alerts:
•  Enable CloudWatch metrics and logs for each job.
•  Add EventBridge rules or CloudWatch Alarms on job failure to notify the on-call
channel.
•  Optional hardening:
•  If I need time-based automation, I create an EventBridge schedule to start the
workflow daily with the right date parameter.
•  Use Glue Catalog partitions and data quality checks (e.g., row count thresholds)
between steps; fail fast if thresholds are not met, so downstream steps don’t
run with bad data.
This gives a clean, deterministic chain: Clean → Aggregate → Load, with clear success
conditions and parameters flowing end to end.
All r ights reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q12. If Job B fails in a workflow, how would you design retries so the pipeline doesn’t restart
from scratch? 
I make retries localized to the failing job and ensure each job is restart-safe. Concretely:
•  Configure retries at the job level:
•  Set Job B’s “number of retries” to a sensible value (for example, 2–3) with
exponential backoff. This lets transient issues clear without touching Job A or C.
•  Use conditional triggers and failure branches:
•  Keep the success chain A → B → C.
•  Do not auto-restart the workflow; let the workflow pause at B on failure.
•  After fixing the issue (or automatically on next scheduled run), re-run only B
using the same workflow run properties (date/run_id) so it processes the same
input.
•  Make B idempotent and checkpointed:
•  Inputs: B should only read success-marked outputs from A (so reruns see the
same stable inputs).
•  Outputs: write to a temp/staging location, then atomically move/rename to final
(or write to a partition with a success marker). On retry, B detects partial old
attempts and cleans/overwrites that partition before writing.
•  If B writes to a warehouse, use MERGE/UPSERT semantics keyed by a
deterministic batch_id or natural keys, so repeated attempts don’t duplicate
rows.
•  Use a progress manifest:
•  Keep a small DynamoDB table (or S3 manifest) keyed by run_id + task shard.
Mark completed shards for B. On retry, B skips shards already marked done and
only processes the missing ones.
•  Isolate state via parameters:
•  Pass run_id and processing_date from the workflow to all jobs. B uses these to
find the exact input/output paths. This guarantees that re-running B doesn’t
accidentally target a new day’s data.
•  Add guardrails and observability:
•  CloudWatch alarms on B’s error rate.
•  Structured error logs so I can quickly see which shard/partition failed.
•  Optional dead-letter pattern: if B still fails after N retries, emit an EventBridge
event to open a ticket/notification, but keep A’s success intact and do not roll
back the entire workflow.
•  Resume C only after B success:
•  The trigger for C requires B’s success condition. Once B finally succeeds (after
retries), C runs automatically with the same parameters—no need to re-run A.
This approach keeps retries local to Job B, avoids re-running completed steps, and ensures the
pipeline can safely resume from where it failed without duplicating data.
 



## Q13. How would you trigger a Glue Workflow automatically when a new file arrives in S3? 
I use S3 → EventBridge to fire the workflow, with light filtering and a small guard to avoid
duplicate/partial files.
Practical steps:
•  Turn on S3 event delivery to EventBridge for the bucket.
•  Create an EventBridge rule:
•  source = aws.s3
•  detail-type = Object Created
•  filter by bucket name
•  filter by object key prefix/suffix (for example, prefix = raw/events/, suffix =
.parquet)
•  ignore folder markers or temp paths (exclude keys containing tmp/,
_temporary/, or =_SUCCESS).
•  Set the rule’s target to “AWS SDK” → Glue → StartWorkflowRun. In input transformer,
pass dynamic params like:
•  bucket, key (from the event)
•  processing_date derived from the key (if you partition by dt=YYYY-MM-DD)
•  a run_id (EventBridge event id)
•  Make the workflow idempotent:
•  Clean job reads the exact S3 key from parameters, or the exact partition.
•  Write outputs to a deterministic path (for example, s3://…/clean/dt=YYYY-MMDD/)
and create a _SUCCESS marker only after a complete write.
•  Aggregate/Load only consume success-marked data, so a re-fire won’t corrupt
results.
•  Add a small safety buffer if files are uploaded via multipart:
•  In the Clean job, re-check size > 0 and use a short object-exists + ETag-stability
check (or a 1–2 minute delay in EventBridge via a second rule) to avoid reading a
file that’s still being uploaded.
•  For burst control:
•  If you expect many small files, route S3 events → SQS → Lambda, batch keys, and
then StartWorkflowRun once per batch. This keeps Glue from over-scheduling.
•  Monitoring and alerts:
•  CloudWatch metric filters on workflow run failures.
•  EventBridge rule for Glue Workflow state change → SNS/Slack.
This gives a simple, no-Lambda (or minimal-Lambda) path: new file → EventBridge rule →
StartWorkflowRun with the right parameters, plus filtering so only the right files trigger runs.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q14. How would you combine Glue Workflows with Step Functions to get centralized failure 
monitoring? 
I keep Glue Workflows for data-step orchestration inside a domain, and use Step Functions as
the top-level controller for runs, retries, and alerts—all visible in one place.
How I set it up:
•  One Step Functions state machine per pipeline family.
•  First task: “Start Glue Workflow Run” using the AWS SDK integration
(glue.startWorkflowRun) with parameters like processing_date and run_id.
•  Poll for completion:
•  Add a loop: Wait (for example, 60–120 seconds) → GetWorkflowRun
(glue.getWorkflowRun) → check status.
•  Exit on Succeeded; go to Catch on Failed/Stopped/TimedOut.
•  Centralized retries:
•  In the Step Functions task that polls, set a Retry policy (max attempts,
exponential backoff) for transient API errors.
•  Do not restart the entire pipeline on business failures; let the Glue Workflow
itself keep job-level retries local to the failing job.
•  Centralized failure handling:
•  Use a Catch block that publishes to SNS/Slack, creates a ticket, or writes an
incident record to DynamoDB.
•  Include the workflowRunId, failing node, and processing_date in the alert
context so the on-call can re-run the exact slice.
•  Timeouts and SLAs:
•  Put an overall timeout in Step Functions (for example, 3 hours). If exceeded,
mark the run as failed centrally even if Glue is still busy, and alert.
•  Drill-down visibility:
•  Step Functions gives an execution history and one “red/green” status per run.
•  From the alert or dashboard, link to the Glue Workflow run page and
CloudWatch logs of the failing job for details.
•  Optional: multiple workflows:
•  If the pipeline has stages owned by different teams, orchestrate each stage as
its own Glue Workflow. Step Functions runs them in sequence or in parallel,
with per-stage Catch/Retry, keeping one pane of glass for status and alerts.
This structure lets Glue Workflows do what they’re best at (job chaining and data-aware steps),
while Step Functions provides a single control point for monitoring, retries, timeouts, and
notifications across the whole pipeline.
 



## Q15. How would you configure a job to run only after multiple upstream jobs complete 
successfully? 
I use a Glue Workflow with a conditional trigger that waits for all required upstream jobs to
succeed.
Practical setup:
•  Create one Glue Workflow that contains all jobs.
•  Add the upstream jobs (for example: Ingest, Clean, Validate). Set them to run in parallel
if they do not depend on each other.
•  Create a conditional trigger for the downstream job (for example: Aggregate). In the
trigger, select “on success” of Ingest, Clean, and Validate. The trigger condition uses an
AND rule, so Aggregate starts only when all three report Succeeded.
•  Pass a single set of workflow-run parameters (for example, processing_date and run_id)
to every job so they all operate on the same slice of data.
•  Make upstream jobs write a success marker after finishing, and write outputs to
deterministic, date-partitioned paths. The downstream job reads only success-marked
partitions. This avoids starting on partial data if an upstream job left temporary files.
•  Keep retries local to each upstream job. If one fails, fix or retry only that job; the
downstream trigger will not fire until all are green.
•  Add CloudWatch alarms on each job’s failure metrics and a workflow-level failure event
to notify the team.
This gives a clean fan-in pattern: multiple upstreams can run in parallel, and the dependent job
starts automatically only when all inputs are ready and successful.
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
All rights  reserved. Personal use only. Redistribution or resale is prohibited  
 
 
 



## Q43. You have a Glue workflow with three jobs (Clean → Aggregate → Load). How would you
ensure the Load job doesn’t run if the Aggregate job fails? 
I tie job execution to upstream success and add a defensive data check so Load can’t start
unless Aggregate actually produced valid output. 
• Workflow dependencies
In the Glue Workflow, I create two triggers:
– Trigger 1: starts Aggregate only when Clean has SUCCEEDED
– Trigger 2: starts Load only when Aggregate has SUCCEEDED
I do not use “Any state” triggers—only “Succeeded”. I also set retries/timeouts on
Clean/Aggregate so they either succeed within bounds or surface a failure that blocks Load.
• Defensive handoff (belt-and-suspenders)
Even with triggers, Load verifies that Aggregate produced what we expect:
– Check for a success marker (for example, _SUCCESS or a manifest file with row counts) in the
Aggregate output prefix
– Optionally, validate record count > 0 and schema hash matches
If these checks fail, Load exits gracefully without writing, which protects downstream systems if
someone runs Load manually by mistake.
• Orchestration alternatives for richer control
If I need branching/alerts, I wrap the three steps in Step Functions:
– Clean → Aggregate → Choice: if Aggregate status == SUCCEEDED → Load; else → notify/stop
This gives clear failure paths and notification hooks.
• Idempotency and recovery
Aggregate writes to a staging location and only promotes/renames on success. Load reads only
the promoted location, so a partial Aggregate never triggers a bad Load even if someone forces
it.
Result: Load can only start on an Aggregate SUCCEEDED event, and a quick runtime sanity
check prevents accidental runs or empty loads.
All ri ghts reserved. Personal use only. Redistribution or resale is prohibited  
 

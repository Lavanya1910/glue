# MODULE 3: INCREMENTAL PROCESSING


## Q8. Your Glue job keeps reprocessing the same files into Redshift. How would you use Job 
Bookmarks to fix this? 
 
I enable bookmarks, make sure the source node is bookmark-aware, and keep the job identity
stable so Glue can remember what it has already read.
• Turn on bookmarks on the job:
•  Job parameter: --job-bookmark-option=job-bookmark-enable (use pause if you need a
temporary backfill, disable to turn it off).
• Read from S3 using a bookmark-aware source and give it a stable transformation_ctx:
src = glueContext.create_dynamic_frame.from_catalog(
database="raw_db",
table_name="transactions_raw",
transformation_ctx="src_tx" # important for bookmarks
)
Glue will track S3 object path + last modified so unchanged files aren’t re-read.
• Keep the same job name between runs. If you rename or recreate the job, you lose the
bookmark state and it will re-ingest.
• Don’t point the reader at giant, changing roots. Keep a stable prefix (e.g., partitioned by
date/hour). If you rewrite old files, Glue may see them as “new” due to modified timestamps—
avoid file overwrites in processed partitions.
• For JDBC sources, set bookmark keys (e.g., updated_at) so only new rows are read; for S3, no
keys are needed—bookmarks work off the file metadata.
• Idempotent load into Redshift:
•  Land into a staging table, then MERGE into the target on a business key to prevent
duplicates if a reprocess ever happens.
•  Optionally store a high-watermark (max event_time) in a control table for extra safety.
• Operations you’ll actually use:
•  Reset bookmarks after a one-time backfill: --job-bookmark-option=job-bookmark-reset.
•  Pause bookmarks to reprocess a window: --job-bookmark-option=job-bookmarkpause.
•  Monitor bookmark metrics and log how many files/bytes were considered “new” each
run.
With bookmarks enabled, a stable source context, and an idempotent Redshift load pattern,
the job stops reprocessing the same S3 files.
 



## Q23. How would you stop a Glue job from reprocessing all history daily when only new S3 
files need to go into Redshift? 
I make the pipeline incremental end to end: only read new files from S3, load them into a
Redshift staging table, and upsert into the target so duplicates never appear.
1.  Read only new S3 objects
• Enable Glue job bookmarks and keep a stable S3 path layout. Bookmarks track which
objects were already processed, so each run picks only new ones.
• Organize data in date-based partitions (for example, s3://bucket/table/dt=YYYY-MMDD/).
Put an early filter on the latest partitions so the job doesn’t list old paths.
• If producers sometimes rewrite files, I add a processed-keys ledger (for example, in
DynamoDB) keyed by S3 object ETag + path; I skip keys already recorded to avoid rereads
when bookmarks can’t help.
2.  Validate idempotency before loading
• Add a unique event_id or file_id + row_number to each record. Drop duplicates within
the batch to protect Redshift from accidental replays.
• Keep small batches: compact tiny source files upstream so the job handles fewer,
larger files each day.
3.  Stage, then merge in Redshift
• COPY only the new batch into a narrow staging table with the same schema as the
target.
• Run a single MERGE statement from staging to the target using a business key or
event_id. When matched, update; when not matched, insert. This gives upsert behavior
and prevents duplicates if a file is retried.
• After a successful MERGE, truncate the staging table and, if I keep a ledger, mark
those S3 keys as processed.
4.  Operational safeguards
• Keep bookmarks enabled and never change the input path without intention.
• Add a guardrail filter like dt >= today-1 to avoid accidental full scans.
• Alert if the job reads more than an expected max number of files or bytes; that’s
usually a sign bookmarks or filters slipped.
Result: the job does not reread historical data, Redshift only sees the delta, and replays cannot
create duplicates.
 



## Q42. Your Glue ETL job runs daily. How would you ensure only new S3 files are processed 
and avoid reprocessing old data? 
I combine Glue job bookmarks with strict path conventions and idempotent writes. The job
reads only files it hasn’t seen, and even if a retry happens, the sink won’t duplicate.
• Enable and respect Glue bookmarks
Turn on job bookmarks (job parameter job-bookmark-option=job-bookmark-enable). Read S3
using Glue’s DynamicFrame APIs (from_catalog or from_options) so bookmarks track
processed object keys. Keep the input prefix stable; moving files breaks bookmarks.
• Date-partitioned paths + early filters
Organize data as s3://bucket/dataset/dt=YYYY-MM-DD/ and filter on just today/yesterday
before any joins. This reduces listing and makes bookmarks faster. Use pushDownPredicate
(DynamicFrame) to limit to the intended partitions.
• Guardrail on first run vs subsequent runs
On day 1, let it read a bounded backfill range. After that, remove any “force” options and rely on
bookmarks. Don’t mix spark.read directly for sources you expect bookmarks to manage.
• Idempotent sink pattern
Write to a staging location, then MERGE into the target (Delta/Hudi/Iceberg on S3, or Redshift
via COPY to staging + MERGE). Use a unique event_id or file_id + row_number so duplicates are
naturally ignored on retries.
• Handle upstream rewrites and duplicates
If producers occasionally rewrite the same object, maintain a lightweight ledger (DynamoDB)
keyed by S3 object key + ETag. Skip if already seen. This complements bookmarks for edge
cases.
• Operational hygiene
Protect the bookmark/ checkpoint S3 prefix from lifecycle deletion. Alert if the job suddenly
reads far more files than expected (a sign bookmarks or filters slipped). Keep one active job per
dataset to avoid racing updates to the bookmark state.
Result: the daily run scans only the new partition’s files, bookmarks ensure each object is
processed once, and the sink’s MERGE guarantees no duplicates even if a batch is retried.
 

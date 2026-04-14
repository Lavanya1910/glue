# MODULE 5: STREAMING ETL


## Q16. How would you design a Glue Streaming Job that reads from Kinesis and writes to S3 in 
Parquet with low latency? 
I build a Spark Structured Streaming job on Glue Streaming that reads Kinesis Data Streams,
does light transforms, and writes Parquet to S3 with a small micro-batch interval and proper
checkpointing.
Key design points:
1.  Source and schema
•  Use the Kinesis Data Streams source with starting position set to LATEST for new
pipelines, or TRIM_HORIZON if I need historical replays.
•  Define the schema up front for speed and stability. If messages are JSON, I parse with
from_json using a fixed StructType. If the producer uses Glue Schema Registry
(Avro/Protobuf), I deserialize with the registry library so schema evolution is managed.
2.  Exactly-once and checkpoints
•  Configure a unique checkpointLocation in S3. This stores Kinesis offsets and batch
state, so the job can restart without duplicating data.
•  Use deterministic output paths and rely on Structured Streaming’s batch-idempotency
so each micro-batch writes once even after retries.
3.  Low latency tuning
•  Set a short trigger interval (for example, processingTime = 30 seconds). I go lower (5–15
seconds) only if the business really needs it.
•  Right-size workers to shard count. As a rule of thumb, at least one executor per 1–2
Kinesis shards. Choose WorkerType G.1X or G.2X and set numberOfWorkers based on
throughput tests.
•  Tune Kinesis read options to balance latency and throughput (for example, limit pershard
fetch to avoid large spikes).
•  Keep transformations lightweight. Avoid wide shuffles inside the streaming job. If I must
aggregate, use watermarks and time windows to bound state, or move heavy
aggregations to a separate batch job.
4.  Durable, analytics-friendly S3 writes
•  Write to S3 in Parquet with Snappy compression. Partition by high-cardinality-but-stable
keys like dt=YYYY-MM-DD and optionally hour if needed for querying and lifecycle
policies.
•  Expect many small files due to low-latency batches. Schedule a separate compaction
job (hourly or daily) that merges small Parquet files into larger ones (for example, 128–
512 MB) for Athena/EMR performance.
•  Maintain a Glue Catalog table over the curated path so Athena/Redshift Spectrum can
query it. Update partitions automatically using a crawler or by programmatically adding
partitions at the end of each batch.
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 
5.  Backpressure, errors, and DLQ
•  Enable backpressure by keeping the trigger small and making sure executors have
headroom. Monitor Kinesis iterator age; if it grows, add workers or reduce per-batch
work.
•  For bad records, use a try-parse pattern: separate good rows and bad rows; write bad
rows to an S3 dead-letter path with the original payload and error for later reprocessing.
6.  Reliability and operations
•  Put the job in “continuous” mode with automatic restarts on failure. Keep checkpoints
in a stable S3 location that is not rotated.
•  Add CloudWatch dashboards for Glue job metrics, Kinesis iterator age, and S3 error
rates. Set alarms on failures and rising iterator age.
•  Use IAM least privilege: read from the Kinesis stream, write to target S3 prefixes, write to
checkpoint and DLQ prefixes, and update the Glue Catalog as needed.
7.  Minimal code structure (conceptual)
•  Read stream from Kinesis with options for region, stream name, and starting position.
•  Parse JSON to columns, add ingestion_time, and derive dt/hour.
•  Write stream to S3 in Parquet with checkpointLocation set, trigger set to the chosen
latency, and partitionBy(["dt","hour"]).
•  Keep the job parameters for stream name, target paths, micro-batch interval, and
checkpoint path, so I can promote across environments without code changes.
This setup gives near–real-time delivery (tens of seconds), safe restarts with no duplicates,
Parquet files ready for analytics, and a path to scale by adding workers or compaction without
redesigning the pipeline.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q17. If Kafka events arrive late, how would you handle them in a Glue Streaming Job so 
reports stay accurate? 
I treat the stream in event time, not processing time, and let late records correct earlier results
without double counting. 
1.  Parse and standardize event time early
I extract event_time from the payload, convert it to a proper timestamp, and standardize
timezone. This becomes the single clock for windows and aggregations.
2.  Use watermarks to accept “late but not too late” data
I apply a watermark on event_time based on the business SLA (for example 1 hour, 6
hours, or 1 day depending on how late events typically arrive). The watermark bounds
state so Spark keeps only the windows that can still change. Anything later than the
watermark is dropped or diverted to a quarantine table for investigation.
3.  Choose the right windowing and keys
For rollups I use tumbling or sliding windows on event_time plus the business key (for
example user_id, product_id). That ensures the same window is updated when a late
event for that key shows up.
4.  Make the sink upsert-friendly so late data can “fix” history
I write to a table format that supports MERGE/updates such as Delta Lake, Apache
Hudi, or Apache Iceberg on S3. I keep a stable composite key like (window_start,
window_end, dimension_keys) for aggregates, or a unique event_id for fact tables. With
foreachBatch I upsert so a late record updates the previous aggregate instead of
creating duplicates.
5.  Keep a deduplication guarantee
If the producer includes a unique event_id, I dropDuplicates by event_id within a
watermark. If not, I create a deterministic id (hash of business keys + event_time +
payload). This protects reports from replays.
6.  Serve reports in a way that tolerates late data
For BI, I either query the upserted lakehouse table directly (Athena/Presto/EMR) or
refresh downstream warehouses from that table. For dashboards, I mark the most
recent, still-open windows as provisional until the watermark closes them.
7.  Operability and guardrails
I monitor state-store size and watermark delay, alert if the lateness distribution shifts,
and periodically review the watermark value. I also keep a dead-letter path for extremely
late or malformed events so I can backfill them with a controlled batch if needed.
Minimal code shape (only to show intent):
•  withWatermark on event_time
•  groupBy window(event_time, "15 minutes") and keys
•  foreachBatch to MERGE into Delta/Hudi/Iceberg by window key
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q18. A Glue Streaming Job reprocesses Kafka offsets after restart, causing duplicates. How 
would you fix it with checkpointing? 
I let Spark recover offsets from a stable checkpoint in S3 and make the sink idempotent so even
if a micro-batch replays, results don’t duplicate.
1.  Use a durable checkpointLocation and never change it for this job
I configure a single S3 path like s3://bucket/checkpoints/glue-stream-job-1/. I ensure
lifecycle policies don’t delete it. This path stores Kafka offsets and state so on restart
Spark continues from the last committed offsets.
2.  Do not set startingOffsets after the first successful run
I might use startingOffsets=latest or earliest only for the initial bootstrap. After that, I
remove startingOffsets and let Spark use the checkpoint. Forcing startingOffsets on
restarts overrides checkpointed offsets and causes reprocessing.
3.  Keep a consistent consumer group
Spark’s Kafka reader uses the checkpoint to manage group offsets. If I change the
checkpoint path or the job identity, Kafka treats it as a new consumer group and
replays. I keep the job name, checkpoint path, and configuration stable.
4.  Make writes idempotent (exactly-once effect at sink)
Best option is an upsert-capable table format. In foreachBatch, I MERGE by a unique
event_id so replayed records update the same rows rather than insert duplicates. If
writing files only, I deduplicate within a watermark using event_id before writing.
5.  Handle transactional producers correctly
If producers use Kafka transactions, I set read_committed so I don’t read aborted
messages. This avoids weird replays when a producer aborts and retries.
6.  Operational hygiene
I run only one active instance of the job per checkpoint path. I protect the checkpoint S3
prefix from accidental deletion. If I must reset the checkpoint (major schema change), I
write to a new sink and backfill once, then switch readers to the new table to avoid
mixing old/new offsets.
In short: stable S3 checkpoint for offset recovery, no startingOffsets after bootstrap, consistent
consumer identity, and an idempotent upsert sink. This combination prevents duplicates after
restarts.
All ri ghts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q19. How would you enrich Kinesis streaming data with a static S3 reference dataset in 
Glue? 
I join the streaming events with a static lookup table that I load from S3 at job start (and refresh
it on a schedule if it changes). The pattern is a stream–static join with a broadcast of the static
data so the join stays fast.
1.  Load and prepare the reference data once
I store the reference table on S3 in a columnar format (Parquet/Delta) with clean keys.
In the Glue job’s initialization code (outside the stream), I read it as a static DataFrame,
select only needed columns, and cache/broadcast it. If it changes daily, I reload it every
N batches inside foreachBatch.
2.  Read and parse the Kinesis stream
I read the Kinesis stream, parse JSON, and extract the join key from the event. I also
normalize timezones and types so keys match exactly.
3.  Do a stream–static broadcast join
Spark allows joining a streaming DataFrame with a static DataFrame. Because the
reference is static and relatively small, I broadcast it to avoid shuffles. For large
reference tables, I keep them in Delta with partitioning and pre-filter before broadcast.
4.  Handle misses and data quality
I mark unmatched keys, route them to a dead-letter path for later fixes, and keep the
main pipeline flowing. If the reference table uses SCD Type 2, I either pre-resolve
effective records into a point-in-time snapshot before the join or handle validity range
inside foreachBatch using event_time.
5.  Refresh strategy for the static table
If the reference changes, I refresh it safely:
•  If changes are infrequent: reload every X minutes or every M batches (e.g., when batchId
% 60 == 0).
•  If using Delta: read the latest version each refresh, so readers get an atomic snapshot.
6.  Write enriched output to an upsert-friendly sink
I write to Delta/Hudi/Iceberg, with a unique event_id for idempotency. If needed, I
upsert aggregates in foreachBatch.
Minimal shape (Scala/PySpark-like, shortened just to show the idea):
•  read Kinesis → parse → eventsDf(key, cols…)
•  refDf = spark.read.parquet("s3://…/ref/").select(key, attrs…).cache()
•  enriched = eventsDf.join(broadcast(refDf), Seq("key"), "left")
•  foreachBatch to write and occasionally refresh refDf if needed
Key Glue considerations
•  Keep reference data compact (only needed columns), use Parquet/Delta, and add ZORDER/
partitioning on the join key if the table is large.
•  Broadcast only when the reference fits in executor memory; otherwise pre-filter by a
Bloom list or split the join by hash buckets.
•  Monitor null join rates; high misses usually mean bad keys or delayed reference
updates.
 



## Q20. Your Glue Streaming Job lags when processing 100k events/sec. What tuning steps 
would you take to scale and reduce latency? 
I scale the source, the compute, and the sink together, then remove bottlenecks in shuffles,
state, and writes. I also cap intake to what the system can stably process.
1.  Ensure enough parallelism at the source
For Kinesis, I provision enough shards. Each shard has throughput limits; to handle 100k
events/sec, I increase shard count so I can read in parallel across many shards. I also
set read options to fetch more per read (for example, increase records per shard per
fetch) while watching latency.
2.  Right-size Glue workers and concurrency
I move to larger/faster workers (e.g., G.2X/G.4X) and increase number of workers so
total cores match or exceed the parallelism from shards. I keep only one streaming job
instance per checkpoint path. I give executors enough memory headroom for state and
broadcast joins.
3.  Tune micro-batch cadence and intake
I pick a small but stable trigger (for example 5–10 seconds) so batches are frequent and
short. I cap the intake per batch to control processing time:
•  For Kafka: set maxOffsetsPerTrigger.
•  For Kinesis: raise per-shard fetch limits only to the point where batch time stays below
trigger interval; don’t overload the batch.
4.  Reduce shuffles and skew
I avoid wide shuffles in the hot path. Where aggregations are required, I pre-aggregate
early (map-side combine), and choose reasonable spark.sql.shuffle.partitions (near 2–
3× total executor cores) instead of defaults. If keys are skewed, I add salting or partial
aggregation to prevent single partitions from becoming hotspots.
5.  Control state size with watermarks
For any stateful operations (windows, dedup), I set watermarks that reflect realistic
lateness so state doesn’t grow without bound. Smaller state means shorter batch times
and fewer GC pauses.
6.  Make the sink fast and idempotent
I write to Delta/Hudi/Iceberg with file sizing tuned for throughput (avoid too many tiny
files). I use foreachBatch to batch upserts/writes and commit fewer, larger files. If
compaction or clustering is heavy, I run it asynchronously in a separate job—don’t block
the streaming writer.
7.  Use efficient compute settings
I enable Kryo serialization, AQE (adaptive query execution), and keep executor heap/offheap
balanced to avoid GC storms. I minimize Python UDFs in the hot path; prefer Spark
SQL or Scala/Java UDFs if needed. I cache only what truly benefits (for example, a small
broadcast reference table).
8.  Storage and checkpoint hygiene
I place checkpointLocation on a fast, dedicated S3 prefix and ensure no lifecycle
deletes it. I also avoid excessive small state files by keeping batch sizes stable. If I see
long checkpoint commits, I reduce parallelism in the sink or increase batch interval
slightly.
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 
9.  Measure, then iterate
I watch these metrics: input rows per second, processed rows per second, batch
duration vs trigger interval, state store memory, shuffle read/write times, and file
commit times. I scale shards/workers until processedRowsPerSecond consistently
meets or exceeds input.
In practice, the biggest wins usually come from more shards (true source parallelism), rightsizing
workers, reducing shuffles, setting tight but safe watermarks, and making the sink write
fewer, larger, idempotent commits.
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 

# MODULE 7: DATA WAREHOUSE & QUERY LAYER


## Q28. How would you integrate Glue with Lake Formation so analysts can query only specific 
columns in Athena? 
I put Glue’s Data Catalog under Lake Formation governance and grant column-level
permissions to analyst roles. Athena then enforces those permissions automatically.
1.  Put the lake under Lake Formation control
• Register the S3 data locations in Lake Formation.
• Turn on Lake Formation permissions for the Glue Data Catalog and set data lake
admins.
2.  Create databases/tables in the Glue Catalog (governed)
• Use crawlers or Glue jobs to create/update tables, but the permissions are managed
in Lake Formation, not directly in Glue.
• Ensure the crawler/job IAM roles have Lake Formation permissions to write metadata.
3.  Define access using LF grants or LF-Tags
• Simple case: grant SELECT on specific columns to an analyst IAM role or group (for
example, allow columns id, date, amount but deny email, phone).
• Scalable case: set up LF-Tags like pii=yes/no, domain=sales, and attach tags to
columns. Grant analysts access via tag expressions (for example, pii=no). This
automatically applies as new columns/tables are added.
4.  Optional row-level filters and masks
• If needed, add row filters (for example, region = 'IN') and column-level masking for
sensitive columns. Athena will enforce both.
5.  Hook up Athena to Lake Formation
• Use an Athena workgroup with Lake Formation integration.
• Grant the analyst role Lake Formation permissions (not just S3/IAM). S3 bucket
policies should allow access “via Lake Formation” so LF can authorize.
6.  Separate roles for engineers vs analysts
• Data engineers’ role gets broader table/column access (and CREATE/ALTER) for ETL.
• Analysts’ role only gets SELECT on approved columns, possibly via LF-Tags.
7.  Audit and lifecycle
• Enable Lake Formation audit logs to see who queried which columns.
• When schemas evolve, new sensitive columns inherit tags/policies, so analysts don’t
see them until explicitly permitted.
With this setup, Glue still manages the schemas, but Lake Formation is the policy brain.
Analysts in Athena only see and query the columns they’re allowed to, with no code changes on
their side.
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q29. How would you design a pipeline where Glue writes streaming data to S3, but analysts 
query it almost in real time via Athena? 
I keep the writer fast and append-only, and I use a table format that Athena can read while data
is still arriving. The target is a 1–5 minute end-to-dashboard delay.
1.  Ingest and write from Glue Streaming
I read the stream (Kafka/Kinesis), parse and clean the events, and write to S3 in Parquet
using a table format that supports atomic snapshots and concurrent reads, like Apache
Hudi or Apache Iceberg. I enable upserts with a unique event_id so retries do not create
duplicates.
2.  Partitioning and small-file control
I partition by a time column that matches query patterns, for example dt=YYYY-MMDD/
hour=HH. I keep micro-batches short (for example every 1–2 minutes) but coalesce
files so each file is at least 128–256 MB. I run background compaction/clustering
(Hudi/Iceberg) on a schedule so the table stays efficient without blocking the stream.
3.  Make new data instantly queryable without crawlers
I avoid waiting for crawlers by using a governed table (Hudi/Iceberg) registered in the
Glue Data Catalog once. Athena reads the latest table snapshot automatically; no new
partitions need to be “added” by a crawler each minute. If I use plain Hive partitions, I
turn on partition projection in the table properties so Athena can resolve recent hour
partitions on the fly.
4.  Event-time handling and idempotency
I set a watermark on event_time and deduplicate by event_id to keep windows accurate
when late events arrive. Because the sink is upsert-capable, late data can correct
recent partitions and Athena sees the fix on the next snapshot.
5.  Access and reliability
I govern access in Lake Formation so analysts can query safely. I keep checkpointing on
S3 stable so restarts resume without reprocessing. I alert if batch duration grows
beyond the trigger interval or if the table compaction falls behind.
Result: Glue streams to an ACID lakehouse table on S3; Athena queries that table with nearreal-
time freshness and without crawler lag, while file sizes and compactions keep query costs
low.
 



## Q27. What’s the most efficient way to load terabytes of transformed data from Glue to
Redshift, and how would you handle the small file problem? 
I avoid JDBC row-by-row inserts and use Redshift COPY from S3. Glue writes the transformed
data to S3 in large, well-sized Parquet files, then I run a COPY into a staging table and MERGE
into the target.
1.  Write to S3 in a Redshift-friendly layout
• Parquet with Snappy or ZSTD compression.
• Repartition/coalesce so files are big (256–1024 MB each) and the count roughly
matches 2–4× the number of Redshift slices. This gives high parallelism without tiny-file
overhead.
• Partition by common filters like dt=YYYY-MM-DD to make reloads and backfills
targeted.
2.  Load using COPY into a staging table
• COPY from S3 with IAM role; Parquet lets Redshift read columns efficiently.
• Disable unnecessary auto encoding during heavy loads if it slows things, then
ANALYZE after load.
• Use a dedicated WLM/queue for loads so user queries don’t compete.
3.  Upsert safely into the target
• Use a staging table that mirrors the target schema.
• Run a single MERGE from staging to target on business keys; then TRUNCATE staging.
This keeps idempotency and lets me rerun a failed batch without duplicates.
4.  Handle the small file problem at the source
• In Glue, before writing: df.repartition(N, col("some_key")) or just coalesce(N) to hit
target file sizes.
• If the upstream creates many tiny files, I run a nightly compaction Glue job that
rewrites to big Parquet files.
• I avoid writing one file per partition per task; I tune spark.sql.shuffle.partitions to a
number that yields the desired file count.
• I keep COPY batches pointed at a manifest listing only the files in the batch, so latearriving
or extra files won’t sneak in.
5.  Table design and maintenance
• Choose sort/dist keys to fit query patterns, or enable Automatic Table Optimization.
• After large loads, run ANALYZE (and VACUUM only if heavy deletes/updates).
• Monitor load times, rows/sec, and skew; if one sort key causes hotspots, adjust.
This approach maximizes throughput (COPY from S3, columnar Parquet) and eliminates tinyfile
drag by coalescing/compacting in Glue before loading.
 



## Q30. How would you join Redshift customer master data with S3 clickstream data in Glue, 
and what schema/performance/cost challenges might arise? 
I avoid pulling large data over JDBC during every run. Instead, I keep a fresh copy of customer
master in S3 and join it with clickstream there, using Glue for the compute.
Design
1.  Move customer master to S3 regularly
I export customer_dim from Redshift to S3 in Parquet (UNLOAD) on a schedule, or
replicate it continuously with DMS into an Iceberg/Hudi table. That gives me a small,
clean, columnar dimension on S3.
2.  Join pattern in Glue
If customer_dim is small to medium, I broadcast it for a fast stream-static or batchstatic
join with the clickstream fact. If it is large, I repartition both datasets by the join
key and use a standard shuffle join with trimmed columns.
3.  Write optimized outputs
I write results as Parquet to S3 in a query-friendly layout (date/hour partitions, 256–512
MB files). If the output feeds Athena or Redshift again, I keep a stable schema and
reasonable file sizes to control costs.
Key challenges and how I handle them
• Schema mismatches
Redshift types (for example, VARCHAR length, DECIMAL precision, TIMESTAMP TZ) may not
match Spark/Parquet. I normalize types in Glue: cast IDs to consistent string/bigint, standardize
timestamps to UTC, and sanitize column names so they are Hive/Athena friendly.
• Slowly changing dimensions
If customer_dim is SCD2, I resolve a point-in-time view before the join
(effective_from/effective_to) so each click maps to the correct customer attributes at that time.
I can precompute the current snapshot daily if “as-of-now” is enough.
• Performance bottlenecks
Pulling from Redshift via JDBC is slow and memory-heavy; that’s why I use UNLOAD/DMS to S3.
For the join itself, I reduce shuffle size by selecting only needed columns, fixing skewed keys
(salting or partial aggregation), and setting shuffle partitions to about 2–3× total executor cores.
I choose worker size for memory-heavy joins and scale worker count for throughput.
• Small files
Clickstreams often generate many tiny files. I coalesce/repartition to target big Parquet files,
and I run periodic compaction if upstream continues to create small files.
• Cost control
I minimize DPU-hours by pruning data by date or customer segments, broadcasting when
possible, and avoiding unnecessary caches/UDFs. I schedule heavy compaction/jobs on Glue
Flex when SLAs allow.
• Governance and PII
I use Lake Formation to mask or restrict PII columns from the joined output. Analysts only see
permitted columns in Athena, while engineers can access the full dataset if needed.
Result: Redshift stays focused on serving queries; S3 holds a clean, query-optimized customer
dimension; and Glue performs the join efficiently and cheaply, producing governed, analyticsready
data.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 

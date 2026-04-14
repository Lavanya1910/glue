# MODULE 2: BATCH ETL DESIGN


## Q6. How would you design a Glue ETL job to keep only the latest session record per user in 
clickstream data? Would you use DynamicFrames or DataFrames? 
 
I’d use Spark DataFrames for the core logic because window functions and performance tuning
are cleaner there. I read via the Catalog as a DynamicFrame, convert to a DataFrame, do the
dedupe with a window, and write back.
1.  inputs and keys
• Source has user_id, session_id, session_start, session_end, updated_at (or
event_time).
• “Latest” is defined clearly up front: usually the max(updated_at) for each user_id, or
max(session_end) if that’s your truth.
2.  incremental read
• Enable Glue job bookmarks or filter by dt/hr to only process new partitions.
• If late data is common, reprocess a small sliding window (for example, last 6 hours)
and dedupe downstream.
3.  dedupe with a window
• Partition by user_id, order by updated_at desc (or session_end desc), pick
row_number = 1.
• If there can be ties, add a deterministic tiebreaker (for example, session_id,
ingestion_ts).
Minimal PySpark skeleton:
from pyspark.sql import functions as F, Window
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
# read (from Catalog)
src = glueContext.create_dynamic_frame.from_catalog(
database="raw_db",
table_name="clickstream_sessions",
push_down_predicate=predicate_for_window # e.g., dt >= ’2025-08-22’
)
df = src.toDF()
# choose "latest" per user
w = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc(),
F.col("session_id").desc())
df_latest = df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 
# write to curated (e.g., Parquet or Iceberg)
glueContext.write_dynamic_frame.from_options(
frame=DynamicFrame.fromDF(df_latest, glueContext, "df_latest"),
connection_type="s3",
connection_options={"path": "s3://lake/curated/sessions_latest/", "partitionKeys": ["dt"]},
format="parquet"
)
4.  performance and correctness
• Repartition by user_id before the window if data is skewed; use salting if a few users
are extremely heavy.
• Select only required columns before the window to cut shuffle size.
• If the target is a slowly changing “latest” table, write to a staging location and then
atomically swap, or use Iceberg and do an overwrite by filter.
• Validate with row counts: total distinct user_id in the output equals distinct user_id in
the input window.
5.  why DataFrames over DynamicFrames
• DataFrames give direct access to Spark SQL window functions, better Catalyst
optimizations, and a larger ecosystem of functions.
• I still use DynamicFrames for easy Catalog IO and then convert back when writing if
needed. This keeps the code concise without giving up Spark capabilities.
 



## Q7. How would you join a small customer dataset with a huge transaction dataset in Glue 
efficiently? 
 
I keep the big scan on the transactions side and broadcast the small customer table so the join
happens map-side without shuffling the big data.
• Read transactions (large) partition-pruned, read customers (small) with only needed columns.
• Convert to Spark DataFrames, cache the small one, and broadcast it.
tx = glueContext.create_dynamic_frame.from_catalog(
database="raw_db", table_name="transactions",
push_down_predicate="dt=’2025-08-22’"
).toDF().select("txn_id","user_id","amount","dt")
cust = glueContext.create_dynamic_frame.from_catalog(
database="ref_db", table_name="customers"
).toDF().select("user_id","segment","country")
from pyspark.sql.functions import broadcast
joined = tx.join(broadcast(cust), "user_id", "left")
• Make sure the small table actually fits in memory on each executor (tens/hundreds of MB is
fine). If it’s larger, reduce it (select only needed columns, dedupe, filter by country/active flags).
• Keep the large side partitioned by the join key if possible, and push down date/hour filters so
you don’t scan history.
• Handle skew: if a few user_ids dominate, enable adaptive execution and skew join, or salt the
hot keys.
• File formats: Parquet/Iceberg for both sides; avoid CSV.
• Resource tuning: set adequate DPUs, and limit shuffle by broadcasting (no massive
repartitions).
• If customers live in Redshift or a DB, extract them first to S3 Parquet as a small dim before the
job; don’t inner-join over JDBC.
• Validate: count distinct user_id before/after, sample the join, and log join stats.
This pattern (broadcast the small, filter the big) gives the biggest win with the least tuning.
 



## Q9. If your S3 dataset has mismatched field names/types compared to Redshift, how would 
you transform it in Glue before loading? 
 
I fix names and types in Glue so Redshift sees clean, compatible columns. I keep it simple: read
via Catalog, standardize, cast, then load.
What I’d do:
•  Start with a clearly defined Redshift target schema (names, types, nullability,
timezones).
•  Read S3 data through the Glue Catalog to get basic structure, then convert to a Spark
DataFrame for precise control.
•  Rename columns to match Redshift exactly (snake_case, avoid reserved words like
“user”, “timestamp”), and trim weird characters or spaces.
•  Cast types to Redshift-friendly ones: use
boolean/int/bigint/decimal(precision,scale)/timestamp without timezone. Parse strings
to timestamps and numbers; normalize enums/text values.
•  Normalize time to UTC before casting to timestamp. Parse ISO-8601 or epoch safely.
•  Handle nulls/defaults and widen/narrow types thoughtfully (e.g., string → decimal(18,2),
string → bigint). If downcasting, guard against overflow with filters or safe casts.
•  Validate with quick checks (row counts, null ratios, bad-cast counts) before writing.
•  Load to a staging table in Redshift, then merge into the final table.
Minimal PySpark sketch:
from pyspark.sql import functions as F, types as T
src = glueContext.create_dynamic_frame.from_catalog(
database="raw_db", table_name="orders_raw", transformation_ctx="src"
).toDF()
df = (src
.withColumnRenamed("OrderID", "order_id")
.withColumnRenamed("TotalPrice", "total_amount")
.withColumn("order_id", F.col("order_id").cast("bigint"))
.withColumn("total_amount", F.col("total_amount").cast(T.DecimalType(18,2)))
.withColumn("created_at_utc",
F.to_utc_timestamp(F.to_timestamp("created_at_str", "yyyy-MM-dd’T’HH:mm:ssX"),
"UTC"))
.drop("created_at_str")
)
 
# Optional: handle bad rows
df_ok = df.filter(F.col("order_id").isNotNull() & F.col("total_amount").isNotNull())
glueContext.write_dynamic_frame.from_jdbc_conf(
frame=DynamicFrame.fromDF(df_ok, glueContext, "df_ok"),
catalog_connection="redshift_conn",
connection_options={"dbtable": "stg_orders", "database": "analytics"},
redshift_tmp_dir="s3://company-temp/glue-redshift/"
)
Extra things I watch:
•  Decimal precision/scale must fit Redshift target.
•  Strings that should be booleans (“true/false”, “Y/N”) get mapped explicitly.
•  If the source schema drifts, I put a mapping layer (ApplyMapping or a select with explicit
casts) that fails fast on unexpected fields and alerts.
•  I avoid reprocessing by enabling job bookmarks and by always landing in a staging table
followed by a MERGE into the final table keyed on a business key (e.g., order_id).
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q10. You have deeply nested JSON with arrays/structs. How would you flatten it in Glue, and 
when would you prefer DynamicFrames over DataFrames? 
 
If I need a single flat table, I explode arrays and pull struct fields up. If the data is truly
hierarchical, I create multiple related outputs (fact + child tables). I choose the tool based on
control vs convenience.
How I flatten with DataFrames (most control, best performance for complex logic):
•  Read via Catalog → convert to DataFrame.
•  Use explode_outer for arrays to avoid losing rows when arrays are empty.
•  Pull struct fields with dot notation and alias them.
•  Repeat explode for nested arrays; if there are multiple arrays, flatten in stages to avoid a
blow-up in row counts.
•  Add surrogate keys (e.g., event_id) before exploding so I can join child tables back if
needed.
Sketch:
from pyspark.sql import functions as F
df = glueContext.create_dynamic_frame.from_catalog(
database="raw_db", table_name="events_json"
).toDF()
# Example: events has user struct and items array<struct<sku,qty,price>>
df1 = df.withColumn("item", F.explode_outer("items")) \
.select(
"event_id",
F.col("user.id").alias("user_id"),
F.col("user.country").alias("user_country"),
F.col("ts").alias("event_ts"),
F.col("item.sku").alias("item_sku"),
F.col("item.qty").alias("item_qty"),
F.col("item.price").alias("item_price")
)
 
If I need multiple outputs (star schema):
•  Create events_head (one row per event).
•  Create events_items (one row per event-item).
•  Optionally, events_attributes using map_from_entries and explode if there’s a dynamic
attributes map.
How I flatten with DynamicFrames (quickest to get many child tables from nested JSON):
•  Use the built-in Relationalize transform, which automatically generates a set of flat
tables (DynamicFrames) with keys that preserve relationships.
•  Good when the JSON structure is variable and I need to land several related tables
without hand-writing explodes.
Sketch:
from awsglue.transforms import Relationalize
root = glueContext.create_dynamic_frame.from_catalog(database="raw_db",
table_name="events_json")
rel = Relationalize.apply(frame=root, name="events")
# rel is a dict-like; write each table
for name in rel.keys():
glueContext.write_dynamic_frame.from_options(
frame=rel[name],
connection_type="s3",
connection_options={"path": f"s3://lake/curated/events_rel/{name}/"},
format="parquet"
)
When I prefer DataFrames:
•  I need strong performance tuning (pruning columns early, controlling shuffles).
•  I need complex business rules, custom dedupe, conditional transforms, or carefully
staged explodes.
•  I want to integrate window functions and advanced Spark APIs.
When I prefer DynamicFrames (Relationalize):
•  I want a fast path to break a nested payload into multiple flat outputs with referential
keys.
•  The schema changes often and I’m okay with the auto-generated table names/keys.
•  I’m prioritizing speed-to-production over hand-optimized transforms.
 
Operational notes I always apply:
•  Limit the scan with partition predicates; select only columns I actually need before
exploding.
•  Beware of multiple large arrays causing Cartesian growth; sometimes it’s better to keep
separate child tables than to fully flatten into one giant table.
•  After flattening, cast to Redshift-compatible types and write Parquet to S3 for a fast
COPY/MERGE.
•  If analysts query in Athena, I may keep nested Parquet for some use cases and only
flatten fields required for Redshift.
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 



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
 

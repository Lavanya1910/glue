# MODULE 6: PERFORMANCE & COST OPTIMIZATION


## Q22. If a Glue job fails with out-of-memory errors on joins, how would you choose the right 
worker type while balancing cost vs performance? 
I first reduce the memory pressure with join strategy and data shaping. Then I choose a worker
type that gives enough memory per executor for the unavoidable shuffle, without overspending.
1.  Shrink the join before throwing hardware at it
• Prefer broadcast hash join when one side is small enough. I broadcast() the small
table so there’s no massive shuffle.
• If both sides are big, I pre-filter early, select only needed columns, and repartition both
sides on the join key to get balanced partitions.
• Fix skew: add salting for hot keys or use skew join hints so one key doesn’t explode a
single partition.
• Use Bloom filters or semi-joins to reduce the large side before the main join.
These steps often remove the OOM without changing workers.
2.  Right-size partitions to the hardware
Too few partitions → big partitions that don’t fit in memory. Too many → overhead. I set
spark.sql.shuffle.partitions to about 2–3× total executor cores, and I avoid creating huge
rows (wide structs) by trimming columns early.
3.  Pick a worker class based on memory need, not just CPU
• Start with a mid tier (for example, G.2X) for sizable joins.
• If OOM persists due to wide rows or very large shuffles, move to a higher-memory tier
(G.4X or G.8X) so each executor has more heap for shuffle and join state.
• Increase number of workers when I need more parallelism; move up a worker size
when I need more memory per task. I do one change at a time and re-measure.
4.  Balance cost using a simple decision rule
• If the job is failing due to not enough memory per partition, upgrade worker size (G.2X
→ G.4X).
• If it’s simply slow but stable, add more workers of the same size to increase
parallelism.
• After the join, if the job becomes I/O bound, scale down to a cheaper tier and keep
more workers. If it remains memory bound, keep the larger tier.
5.  Operational safeguards
• Avoid caching large DataFrames unless truly needed.
• Prefer Spark SQL expressions over heavy UDFs in the hot path.
• Monitor GC time, shuffle spill, and peak executor memory in the Spark UI. Spills are
okay; repeated OOM means you still need either more memory per executor or smaller
partitions.
6.  Example upgrade path I use in practice
• Fix join strategy and pruning → test.
• If still OOM, increase shuffle partitions and repartition by join key → test.
• If still OOM, move from a smaller to a larger worker type to increase memory per
executor → test.
• Only after stable, tune worker count to hit the desired runtime at the lowest total DPUhours.
This approach keeps costs under control by first reducing the join’s memory footprint, then
matching the worker type to the true bottleneck (per-executor memory vs overall parallelism).
 



## Q24. Your Glue costs doubled last month. How would you use Glue Flex jobs and tuning to 
cut costs while meeting SLAs? 
I split workloads by urgency, move non-urgent ones to Flex capacity, and tune every job to
reduce DPU-hours. The goal is fewer DPUs for less time, without missing deadlines.
1.  Classify jobs by SLA
• Critical (tight deadline, interactive): keep on standard Glue.
• Non-urgent (backfills, daily rollups, file compaction, metadata maintenance, crawlerstyle
discovery): move to Glue Flex where it’s cheaper but may queue before starting.
Schedule these earlier so they still finish before the SLA window.
2.  Right-size each job
• Pick the smallest worker type that keeps the job stable, then add workers for
parallelism only if needed. If a job is memory bound, upgrade worker size; if it’s just
slow, add more of the same size.
• Set a maximum concurrency so multiple heavy jobs don’t spin up too many DPUs at
once.
• Set job timeout and sensible retry counts so failures don’t burn hours.
3.  Reduce scanned data and shuffles
• Use partition pruning and pushdown predicates so each run reads only the required
partitions and columns.
• Compact small files upstream; target 128–512 MB Parquet files. Fewer files cut
planning and listing time, and reduce DPU-hours.
• Trim columns early, avoid wide rows, and eliminate unnecessary caches.
• Repartition intelligently: set shuffle partitions near 2–3× total executor cores and fix
key skew with salting or partial aggregates.
4.  Make sinks efficient
• When writing to S3, coalesce to reasonable file sizes to avoid tiny-file storms that
trigger more work later.
• For Delta/Hudi/Iceberg, run compaction/clustering as a low-priority Flex job on a
schedule instead of during hot paths.
5.  Use incremental patterns
• Enable Glue bookmarks where applicable so batch jobs only touch new files.
• For JDBC reads, use incremental predicates (for example, updated_at >=
last_watermark) to avoid full pulls.
6.  Optimize development and backfills
• For dev/test, process a sampled subset and run on Flex.
• For historical reprocessing, split the range and run multiple small Flex jobs in parallel
windows rather than one oversized standard job.
7.  Monitor and iterate
• Track DPU-hours per job, input bytes, and runtime trends. Identify top cost drivers and
fix those first.
• Watch queue times for Flex jobs; if a Flex job starts threatening the SLA, promote just
that job back to standard while keeping the rest on Flex.
With these steps—Flex for non-urgent work, smaller and smarter jobs, less data scanned, and
leaner writes—costs drop materially while SLAs remain intact.
 



## Q25. If Athena queries on Parquet output are still slow/expensive, what Glue optimizations 
(e.g., partitioning, bucketing, compression) would you apply? 
I reduce how much data Athena has to scan and how many files it touches. I also write Parquet
in a way that lets Athena skip more row groups.
1.  Partition on the columns used most in WHERE clauses
Typical choices are date (dt=YYYY-MM-DD) and one low-to-medium cardinality
dimension like region or source. I avoid high-cardinality columns (like user_id) as
partitions because they create too many small folders.
2.  Push filters and select columns early in Glue
I keep only the columns I need and write them back as Parquet. Athena then benefits
from column pruning and predicate pushdown automatically.
3.  Right-size files and row groups
I target 256–512 MB per Parquet file and ~128 MB row groups. Fewer, larger files speed
up listing and reduce per-file overhead. I run a small compaction job if I have many tiny
files.
4.  Use fast compression that keeps pages small
For Parquet I prefer ZSTD (best scan speed vs size) or Snappy (very common and fast). I
avoid GZIP for Parquet because it slows reads. I set the codec once in Glue so every
new file is consistent.
5.  Sort data by common filter columns before writing
If I sort within each partition by a frequent filter (for example dt, then customer_id),
Parquet min/max stats let Athena skip more row groups. This is a cheap win without
changing the table layout.
6.  Keep partition keys clean and simple
Partition columns must be plain strings like dt=’2025-08-01’. In queries I filter on the raw
partition columns (no functions on them) so Athena prunes folders correctly.
7.  Consider a second-level layout instead of bucketing
Hive bucketing gives limited benefits in Athena and is easy to misconfigure. I prefer a
two-level partition like dt/region and good sorting. If I absolutely need bucket-style joins,
I ensure both tables use the same bucket key and count, but I only do this after
measuring.
8.  Avoid full table scans from Athena
I add a table property for partition projection (when partitions are predictable like
dates). Then Athena can discover partitions on the fly without crawling, and queries that
filter dt won’t waste time on the metastore. If I keep crawlers, I also add a Glue partition
index for very large tables so partition lookups are fast.
9.  Use CTAS/INSERT INTO to optimize after the fact
If I inherit a messy table, I run an Athena CTAS (or a Glue job) to rewrite into an
optimized layout: fewer columns, better partitions, sorted files, and ZSTD/Snappy
compression. Then I point reports to the optimized table.
Result: Athena scans fewer bytes, opens fewer files, and finishes faster, which directly reduces
cost.
 



## Q21. How would you speed up a Glue job on billions of rows in S3 using partition pruning and 
pushdown filters? 
I make the job read as little data as possible by organizing the S3 data correctly and pushing
filters down to the file scan so Spark skips whole folders and whole row groups.
1.  Store data in a partition-friendly layout
I keep the big table in Parquet/ORC with Hive-style partitions such as dt=YYYY-MMDD/
region=IN/. I only partition by columns that are used very often in WHERE clauses
(date, region, tenant). I avoid over-partitioning with high-cardinality fields like user_id.
2.  Write queries that let Spark prune partitions
Partition pruning only works if the filter uses the exact partition column name. So I
always filter like where dt = ’2025-08-01’ and region = ’IN’ before any transformations. I
never wrap the partition column in functions (no to_date(dt)), and I push these filters as
early as possible in the job.
3.  Use Glue’s pushdown predicate when reading from the Catalog
If I use DynamicFrames, I pass a pushdown predicate so Glue only lists and reads
needed partitions:
•  from Catalog: create_dynamic_frame_from_catalog(..., push_down_predicate="dt >=
’2025-08-01’ and region in (’IN’,’US’)")
•  from S3 directly: create_dynamic_frame_from_options(..., pushDownPredicate="...")
This prevents the driver from listing millions of partitions and speeds up planning.
4.  Turn on file-level predicate pushdown and column pruning
Parquet/ORC can skip whole row groups when filters are on regular columns too (not
just partitions). I enable filter pushdown and select only required columns at the top of
the script. In code, I first select() the exact columns I need, then filter() so Spark can
push both projection and predicates to the file scan.
5.  Reduce small-file overhead
Billions of rows often means millions of small files. I compact data during writes (target
128–512 MB Parquet files) and run periodic compaction in a separate job. Fewer, larger
files make pruning more effective and speed up reads.
6.  Use partition indexes for very large catalogs
On very heavily partitioned Glue tables, I add a partition index so GetPartitions calls
return fast when I apply partition filters. This avoids slow plan times and occasional
driver pressure.
7.  Verify pruning actually happens
I check the Spark plan (explain) or Spark UI to confirm the partition filter is applied and
the input bytes are a small fraction of the table. If I still see huge input, I fix the filter
expression or the table’s partition keys.
Result: with correct partitions + pushdown predicates + column pruning, the job scans only the
tiny slice of data for the requested dates/regions and finishes much faster.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 

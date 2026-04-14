# MODULE 1: INGESTION & CATALOGING## Q1. How would you use Glue components (Jobs, Crawlers, Catalog, Triggers) to move raw 
data from S3 to Redshift automatically every hour? 
 
If I had to build this pipeline, I’d set it up in a way where each Glue component plays a very
specific role.
First, the raw data is coming into S3. I’d organize the bucket with partitions by date and hour,
something like s3://bucket/raw/topic/dt=YYYY-MM-DD/hr=HH/. This makes it easier to process
incrementally instead of scanning the entire bucket each time.
Next, I’d configure a Glue Crawler to scan that S3 path and create or update a table in the Glue
Data Catalog. The crawler helps me avoid manually defining schemas, and I can choose to run
it either every hour before the job or maybe once a day if the schema doesn’t change often. The
Catalog then becomes the central metadata store that the Glue job can reference.
Then comes the Glue Job. I’d create a PySpark-based Glue ETL job that reads from the Catalog
table, applies any required transformations like casting datatypes, dropping duplicates, or
filtering out bad records, and then writes the cleaned data into Amazon Redshift. I’d use a Glue
Redshift connection with temporary S3 staging, so the job essentially does a COPY under the
hood, which is faster than row-by-row inserts. I’d usually write into a staging table in Redshift
first, and then merge it into the final reporting table to handle duplicates or late-arriving data.
To automate this, I’d use a Glue Trigger with a schedule that runs every hour. The trigger can
also be chained — for example, first run the crawler, then run the ETL job, and finally run a postload
step if needed. With triggers, I don’t have to rely on external schedulers.
On top of this, I’d enable Glue job bookmarks or pushdown predicates to make sure each
hourly job only processes the new partition, not the full history. That keeps the job efficient and
cost-effective.
From a practical side, I’d also make sure Redshift credentials are stored in AWS Secrets
Manager, the Glue job runs inside the same VPC as Redshift for network access, and
monitoring is set up with CloudWatch and SNS to alert if a job fails.
So overall, the flow would look like this:
S3 (raw files) → Glue Crawler updates Catalog → Glue Job transforms and loads into Redshift →
Trigger runs the pipeline every hour.
 



## Q2. Your S3 bucket has millions of partitioned files. How would you make Glue Crawlers 
faster and cheaper? 
 
I’d treat the crawler as a metadata refresher, not a file scanner. The goal is to minimize what it
touches and how often it runs.
1.  Point the crawler at the smallest possible scope
Keep the include path at the partition root you actually load (for example,
s3://bucket/raw/events/dt=). Avoid pointing at the whole bucket. Add exclusion
patterns for old years, backup prefixes, _temporary/, _SUCCESS, and any logs. If you
have multiple datasets, use multiple small crawlers instead of one giant one.
2.  Crawl new partitions only
In the crawler settings, pick the recrawl policy “Crawl new folders only.” This stops it
from re-checking existing partitions every run. Also choose “Update all new and existing
partitions only when the schema changes” to avoid churn on unchanged objects.
3.  Reduce sampling and file-type confusion
Use narrow, correct classifiers (e.g., JSON or CSV with the right delimiter/quote/escape)
and remove generic ones you don’t need. Keep sampling per partition tiny (a couple of
files). The crawler only needs enough to infer schema—don’t let it read tens of files just
to be sure.
4.  Partition deeper, but crawl shallower
Keep a clean, predictable layout (like dt=YYYY-MM-DD/hr=HH). The crawler only needs
to see the top-level partition folders to register them; don’t let it descend into nested
junk. If you have very deep hierarchies, split by dataset so each crawler’s max depth is
small.
5.  Don’t crawl what rarely changes
If the schema is stable, don’t run the crawler hourly. Run it daily or weekly for schema
drift, and handle hourly arrivals without the crawler:
• Read by path in your ETL (from_options) and write to your target.
• Or programmatically add just the new partitions via BatchCreatePartition from a Glue
job or a tiny Lambda. This event-driven registration (S3 PUT → Lambda → Glue
BatchCreatePartition) is usually faster and cheaper than crawling.
6.  Use time-boxed or sliding windows
If you must use a crawler often, keep it on a sliding window prefix, like “current week” or
“last N days.” Move older data under an archived prefix the crawler never scans.
7.  Keep small files under control
Crawlers spend time listing and sampling. Compact tiny files upstream (or with a daily
compaction job) so there are fewer objects to list and sample. Even if you don’t change
the total bytes, reducing object count speeds the listing phase.
8.  Separate schema discovery from partition discovery
Create the table once (manually or with a one-time crawl on a small sample). After that,
stop full crawls. Only register new partitions programmatically. This avoids re-inference
and keeps the table stable.
9.  Limit IAM and network hops
Give the crawler an IAM role scoped only to the paths it needs. Avoid cross-account or
cross-region listings unless required; use resource links or data sharing patterns
instead of crawling remote buckets.
 
10. Operational guardrails
Add tight schedules and timeouts so a crawler can’t run for hours. Alert on long
runtimes or excessive “objects scanned.” Keep separate crawlers per dataset so one
problematic path doesn’t slow everything.
If I summarize the practical path I’d actually take: create the table once on a small sample, set
a narrow crawler (or none), switch to event-driven BatchCreatePartition for hourly partitions,
and reserve the crawler for occasional schema changes. That combination has given me the
biggest speed and cost win on multi-million-file lakes.
 



## Q3. Custom Classifier If the default classifier fails on complex nested logs, how would you 
build a custom Glue classifier? 
 
I first pin down the log structure (JSON, semi-structured text, XML). Then I create a custom
classifier that tells the crawler exactly how to parse it, and I give that classifier higher priority
than the defaults so it’s always picked.
What I do step-by-step:
1.  Capture a small, representative sample of the logs in S3, including edge cases and bad
rows.
2.  Decide the classifier type:
• JSON: when it’s valid JSON but nested or irregular.
• Grok: when it’s text with repeated patterns (e.g., Apache, app logs).
• CSV: when it’s delimited but not standard (e.g., custom separator, escapes).
• XML: if it’s XML with namespaces.
3.  Build the classifier:
• JSON classifier: define a JSONPath that points to the array or object that represents
“one record.” Example: if each file is { "records": [ {...}, {...} ] }, I set JSONPath to
$.records[*].
• Grok classifier: write a grok pattern that captures the fields. Example pattern for a
timestamped log line with level and JSON payload:
%{TIMESTAMP_ISO8601:ts} %{LOGLEVEL:level} \[%{DATA:service}\]
%{GREEDYDATA:msg} payload=%{GREEDYDATA:payload}
Later, in the ETL job, I parse payload as JSON to explode nested fields.
• CSV classifier: specify delimiter, quote, escape, header presence, and custom date formats.
• XML classifier: set an XPath to the repeating element, e.g., //event.
4.  Assign the classifier to the crawler and move it to the top of the classifier priority list. I
also add an include path that is as tight as possible and exclude everything that doesn’t
match (e.g., *.gz only if that’s true).
5.  Run the crawler on the sample first and validate the Data Catalog table: check column
types, arrays/structs, and that partition keys are correctly inferred (I keep partitions like
dt, hr in the S3 path, not in the payload).
6.  Stabilize the schema: if nested fields are volatile, I map them to a stable shape in ETL
(use defaults for missing fields, cast types). The classifier should only discover
structure, not enforce business schema.
7.  Guardrails: set crawler recrawl policy to “new folders only” and sampling to a few files
per partition. Add CloudWatch alarm if the crawler starts touching too many objects
(signals drift).
Minimal code I keep handy for ETL after a grok classifier extracted payload as text:
from pyspark.sql.functions import from_json, col
schema = "user_id string, event_name string, meta struct<os:string, app_ver:string>"
df2 = df.withColumn("payload_json", from_json(col("payload"), schema)) \
.select("*", "payload_json.*").drop("payload","payload_json")
This keeps the classifier simple and pushes deep parsing to the job where I can control types
cleanly.
 



## Q4. Duplicate Tables - A crawler created separate tables for CSV and Parquet files of the 
same dataset. How would you keep only one logical table? 
 
I pick one canonical format and one canonical prefix, and I make the crawler only see that.
Everything else becomes staging or is excluded.
What I do step-by-step:
1.  Choose the “source of truth.” I prefer Parquet as the single table because it’s columnar,
typed, and cheaper to query.
2.  Separate storage paths:
• Move or write CSV to a staging prefix like s3://bucket/dataset_staging/ (short-lived).
• Keep Parquet in s3://bucket/dataset/ with partitions like dt=YYYY-MM-DD/hr=HH/.
3.  Make the crawler deterministic:
• Point the crawler only to the Parquet prefix.
• Add exclude patterns for *.csv and for the staging prefix.
• Set recrawl policy to “new folders only.”
4.  Clean up the Catalog:
• Tag the unwanted CSV table as deprecated, confirm no queries/jobs rely on it, then
drop it.
• If the Parquet table has the right name, keep it; otherwise, rename it to the logical
dataset name and update downstream jobs to the new table.
5.  Enforce the pipeline:
• Ingestion writes CSV to staging.
• A Glue job converts CSV → Parquet and writes into the canonical prefix.
• The crawler (or better, programmatic partition registration) updates the single Parquet
table only.
6.  Prevent re-appearance of duplicates:
• Separate crawlers per dataset with tight include paths.
• Strict exclusion patterns (*.csv, /staging/*, _temporary/, _SUCCESS).
• If multiple teams land files, publish a storage contract: “only Parquet in the curated
path.”
7.  Optional modernization:
• If the team wants ACID and schema evolution, create a single Iceberg table in the
Catalog and always write to that table. Then deprecate both CSV/Parquet ad-hoc
tables. This gives you one logical table regardless of how data arrived upstream.
In short: make Parquet the only curated location, point the crawler only there, drop the
duplicate CSV table, and keep CSV strictly as a transient staging step.
All righ ts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q5. Your lake has Parquet (historical) + JSON (incremental). How would you design 
crawlers/classifiers so Athena can query both together? 
 
I keep discovery per format but make the schemas line up, then expose a single logical view for
consumers.
1.  create two Glue tables with the same business schema
• One crawler for Parquet pointing at the curated historical prefix.
• One crawler for JSON pointing at the incremental prefix. Attach a JSON classifier with
a JSONPath that targets the record array or object per line.
• In both places, enforce identical column names, types, and partitions (for example, dt
and hr in the S3 path). If JSON is loose, map missing fields to null later in ETL.
2.  control crawler behavior to avoid drift
• Recrawl “new folders only.”
• Tight include paths; exclude tmp files, _SUCCESS, and unrelated datasets.
• Keep the JSON classifier at higher priority than default so the crawler never
misclassifies.
3.  give Athena one thing to query
Option A (fastest to roll out): create an Athena view that unions the two tables and
standardizes types.
Option B (preferred long term): add a small Glue job that converts the JSON
incrementals to Parquet on arrival and writes into the same Parquet table/partitions as
historical. Then the crawler (or programmatic partition adds) touches only the Parquet
table, and analysts query one table.
Option C (modern pattern): use an Apache Iceberg table as the single logical table. Land
JSON to a staging area, run a lightweight Glue job to upsert/append into the Iceberg
table. The crawler registers only the Iceberg table; Athena queries it directly.
4.  practical guardrails
• If you must union in Athena, cast JSON columns to the Parquet schema explicitly and
align timestamps/timezones.
• Keep partition keys in the path, not the payload.
• Validate schema parity with a daily check: list columns and types from both tables and
alert on mismatches.
A minimal Athena view when keeping two physical tables:
CREATE OR REPLACE VIEW analytics.events_all AS
SELECT cast_cols(*) FROM raw.events_parquet
UNION ALL
SELECT cast_cols(*) FROM raw.events_json;
Here cast_cols is just shorthand for explicit casts so both sides match perfectly.
 



## Q26. How would you set up Crawlers + Data Catalog so Athena can query new raw S3 
datasets automatically? 
I give each dataset a stable S3 prefix, a scoped crawler with clear include/exclude rules, correct
classifiers, and a schedule or event trigger. Then I manage permissions so Athena can read
immediately.
1.  Organize S3 and naming
I put each dataset under its own prefix like s3://lake/raw/app1/ and use folder-style
partitions if they exist (for example dt=YYYY-MM-DD/). Clear paths make crawling
predictable.
2.  Create a dedicated IAM role for the crawler
The role can list/read only the dataset prefixes and write to the Data Catalog. Least
privilege keeps things safe.
3.  Choose or add the right classifier
For CSV/JSON, I use built-in classifiers and set options (header, delimiter, quote). For
nested JSON or custom logs, I add a JSON/Grok classifier so schemas are inferred
correctly.
4.  Configure the crawler to target a single database and table strategy
I point the crawler at the dataset prefix and set it to “create a single schema for each S3
path.” I also set table name and a table prefix if needed. I use exclude patterns to skip
_temporary/, _SUCCESS, and archives.
5.  Enable partition inference and schema updates
I let the crawler detect partitions from folder names (like dt=...). I allow schema
evolution with caution: add new columns, but do not drop or rename automatically. If I
expect frequent schema drift, I version tables (table_v1, table_v2) and switch
consumers deliberately.
6.  Schedule or event-trigger the crawler
If the dataset lands daily/hourly, I schedule the crawler shortly after data arrival. For
near-real-time discovery, I trigger the crawler using S3 event → EventBridge → start
crawler. This keeps the Catalog fresh without manual steps.
7.  Add a Glue partition index for very large partitioned tables
If the dataset grows to millions of partitions, I add a partition index so Athena and Glue
can look up partitions quickly when queries filter on them.
8.  Set table properties for better querying
I verify the SerDe, input formats, and compression settings match the files. If the layout
is date-predictable, I consider enabling partition projection in the table properties and
then reduce the crawler frequency (or stop crawling partitions entirely).
9.  Grant access via Lake Formation (or direct IAM if not using LF)
I grant SELECT on the database/table to analyst roles so Athena can query immediately.
This avoids “Access Denied” surprises.
10. Monitor and alert
I enable CloudWatch metrics and an SNS alarm on crawler failures or schema changes.
If the crawler detects unexpected schema deltas, I review before promoting to
production tables.
With this setup, as soon as new files land in the raw S3 prefix, the crawler updates the Glue
Data Catalog, and Athena can query the fresh partitions automatically with the correct schema.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q41. If you have datasets in different formats (CSV, JSON, Parquet) across S3 buckets, how 
would you make them queryable in Athena while avoiding duplicate tables in Glue 
Catalog? 
I set up a single, well-governed Glue Data Catalog with clear ownership and a “one dataset →
one table” rule. Then I normalize formats for performance, and I use crawlers carefully so they
update tables instead of creating new ones.
• Catalog design and ownership
Create one database per domain (for example, sales_raw, sales_curated). Define naming
standards like app_dataset_zone (for example, web_clicks_raw, web_clicks_curated). Only a
small set of roles can create tables; everyone else can only read. This prevents random
duplicate tables.
• Raw zone vs curated zone
In raw, I register what actually lands (CSV/JSON/Parquet). In curated, I use Glue ETL/CTAS to
convert everything to Parquet (or Iceberg/Hudi) with consistent schema and partitions so
Athena is fast. Analysts primarily query curated.
• One crawler per dataset, not per person
Point the crawler to the exact S3 prefix of the dataset. Set “Create a single schema for each S3
path,” provide a fixed table name, and use include/exclude patterns to avoid neighboring
folders. Use Recrawl policy = Crawl new folders only. Set Update behavior = Update in
database so it changes the existing table instead of making a new one with a suffix.
• Prevent duplicates by policy
Restrict CreateTable in Glue to a CI role or data engineering role. Everyone else submits a
change request. This stops ad-hoc tables. For multi-account, use Resource Links to reference
the same table instead of copying metadata.
• Handle different formats cleanly
If a dataset arrives in mixed formats, split raw tables by format (for example,
web_events_json_raw, web_events_csv_raw) under the same database, then consolidate to a
single curated Parquet table. Don’t let the crawler infer two schemas into one table.
• Partition strategy and projection
Partition by dt=YYYY-MM-DD (and maybe region). For large, predictable partitions, enable
Athena partition projection so you don’t need the crawler to add daily partitions. That also
avoids churn in the Catalog.
• Schema evolution discipline
Allow adds but block destructive changes. If a breaking change is needed, version the table
(…_v2) instead of letting the crawler mutate columns in-place, which often triggers duplicates
elsewhere.
Result: every dataset has exactly one authoritative table per zone, crawlers only update that
table, and Athena users get stable, fast Parquet tables in curated.
 



## Q45. How would you let analysts query raw, curated, and aggregated S3 data in Athena 
without them manually creating schemas? 
I centralize all schemas in Glue Data Catalog and automate their creation/updates, so analysts
only need SELECT permissions in Athena.
Design
1.  One Glue Data Catalog as the single source of truth. Create databases like raw,
curated, and analytics. Enforce naming standards so datasets are easy to find.
2.  Automate table creation:
• Raw: use one crawler per dataset, scoped to its exact S3 prefix with include/exclude
patterns. Set recrawl policy to “new folders only” and update behavior to “update in
database” so it updates one table instead of creating duplicates.
• Curated/Aggregated: have Glue ETL jobs write Parquet/Iceberg and register or update
tables programmatically at the end of each job (Glue APIs or Spark/Iceberg integration
auto-commits table metadata).
3.  Avoid waiting on crawlers for predictable partitions by enabling partition projection.
Analysts can query new date/hour partitions immediately without manual ALTER
PARTITION or crawler runs.
4.  Give analysts access via Lake Formation. Grant SELECT on the specific
databases/tables (and column-level filters if needed). No one outside data engineering
needs CreateTable privileges.
5.  Publish a data dictionary: lightweight Glue table properties and a Confluence/README
page per dataset (owner, refresh cadence, sample queries). Analysts discover and
query instantly from Athena’s table list.
6.  Keep performance consistent: in curated/analytics, store columnar (Parquet or
Iceberg), partition by dt and another low-cardinality dimension, and target 256–512 MB
file sizes so Athena scans are fast and cheap.
Result: data engineers own schema automation; analysts open Athena, see governed tables
across zones, and query without creating or maintaining schemas themselves.
 



## Q50. How would you configure a Glue Crawler to handle new fields in JSON data without 
breaking Athena queries? 
I let the crawler add new columns safely, never drop/rename, and I shield consumers with
stable views. When evolution is fast, I move the data to Parquet with explicit schemas.
Steps
1.  Use a JSON-aware setup
Create a dataset-specific crawler targeting only the dataset prefix. Use the built-in JSON
classifier or a custom JSONPath classifier if the payload nests deeply. Keep
include/exclude rules tight so it doesn’t pick up unrelated files.
2.  Safe schema change policy
In the crawler’s configuration:
• Schema change policy → “Update in database” and “Add new columns” only.
• Do not enable automatic delete/rename; set delete behavior to log only. This prevents
the crawler from breaking existing schemas by dropping/renaming columns.
3.  Recrawl discipline
Set Recrawl policy = “Crawl new folders only.” For hourly/daily partitions, this avoids reinferring
old data and keeps evolution incremental. Combine with partitioned paths (for
example, dt=YYYY-MM-DD/hour=HH).
4.  Keep queries stable with views
Expose an Athena view that selects the stable set of columns used by dashboards.
When the crawler adds a new optional field, the base table gains a nullable column, but
the view shields consumers until they opt in to the new field.
5.  Control types and defaults in ETL
If types drift (for example, id flips between string and number), add a lightweight Glue
job that normalizes JSON before landing to the raw/curated table (cast to string, coerce
bad values to null). Write Parquet in curated to lock types.
6.  Prefer Parquet in curated
For fast-moving JSON, land raw as JSON, but run a Glue job to write curated Parquet
with explicit schema (nullable new fields). Register the curated table once; Athena
reads it without re-inference. This avoids surprises from schema-on-read.
7.  Testing and promotion
Use a dev crawler on a sample prefix first. If the new column set looks good, run the
prod crawler. Alternatively, register the additional columns programmatically (Glue API)
at the end of the ETL that first sees them, so metadata changes are deterministic.
8.  Guardrails and monitoring
Add Athena/Config checks that alert if the crawler attempts a destructive change or if a
key column’s type changes. Keep crawler IAM minimal, and schedule after data arrival
to avoid half-written files.
Result: new JSON fields appear as nullable columns without breaking existing Athena queries,
analysts continue to use stable views, and curated Parquet provides consistent types and
better performance as the schema evolves.

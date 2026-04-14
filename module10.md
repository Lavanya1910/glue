# MODULE 10: METADATA & SCHEMA GOVERNANCE


## Q44. If you are migrating an on-prem Hive-based Spark ETL pipeline to Glue, how would you 
map Hive Metastore metadata into Glue Data Catalog? 
I inventory the Hive schema, decide which tables can be re-created by crawlers vs which need
exact SerDe definitions, then programmatically create matching databases/tables in Glue so
Spark/Athena see identical metadata.
• Inventory and classify
Export a list of Hive databases, tables, locations, input/output formats, SerDes, partition
columns, and table properties. Classify:
– Standard formats (Parquet/ORC/CSV/JSON with common SerDes) → safe to recreate via
crawler or scripted creates
– Custom/legacy SerDes or complex row formats → script exact definitions (don’t rely on
inference)
• Re-create databases and tables in Glue (programmatic path)
Use a small migration script (boto3 Glue client) that loops through the inventory and calls:
– create_database(name, description, locationUri)
– create_table(...) with StorageDescriptor (columns, SerDeInfo, InputFormat, OutputFormat,
Location), PartitionKeys, TableType, Parameters
This preserves schema, SerDe, file formats, and table properties 1:1. For external tables, point
to the same S3/HDFS locations (after data is moved to S3).
• Crawlers for simple cases
For plain Parquet/ORC datasets laid out as Hive-style partitions, point a crawler at each dataset
prefix with:
– Include/exclude patterns so it doesn’t create duplicate tables
– Recrawl = “new folders only”
– Update behavior = “update in database”
This is faster than scripting for straightforward tables.
• Partitions at scale
If tables have millions of partitions, I avoid importing every partition. Instead I enable partition
projection in Glue/Athena (define ranges/patterns for dt, region, etc.). For moderate partition
counts, I create partitions via batch CreatePartition calls.
• Normalize and fix incompatibilities
– Make column names Glue/Athena-friendly (no spaces, backticks, or uppercase-only
semantics)
– Align types (for example, Hive TIMESTAMP with/without TZ vs Spark/Athena expectations)
– Ensure table locations use s3:// paths; convert HDFS paths during data migration
• Permissions and governance
Put the new Catalog under Lake Formation if used. Grant SELECT (and column-level restrictions
if needed) to consumer principals. Limit Create/UpdateTable to data engineering roles to
prevent drift.
• Validate before cutover
For each migrated table:
– Run SELECT count(*), sample queries in Athena and a small Spark job using the Glue Catalog
– Compare schema and row counts to Hive source
– Only then switch producers/consumers to use the Glue Catalog name
• Spark/ETL pipeline switch
In Spark configs, set the Hive metastore to use the Glue Data Catalog. The ETL code continues
to use the same table names; only the metastore backend changes.
All r ights reserved. Personal use only. Redistribution or resale is prohibited  
 
• Rollback and documentation
Keep the mapping file (hive_db.table → glue_db.table) and a rollback plan. Document any tables
re-modeled in curated form.
Result: the Glue Data Catalog mirrors the Hive Metastore accurately where needed, crawlers
handle simple cases, partition projection prevents metadata blowups, and Spark/Athena can
read the data immediately with minimal code change.



## Q46. If a source keeps adding new optional fields, how would you manage schema changes 
in Glue so Athena/Redshift queries don’t break? 
I design for backward compatibility: treat new fields as nullable, keep stable views for
consumers, and automate safe metadata updates.
Athena/S3 side
1.  Store data in Parquet or an ACID table format (Iceberg/Hudi/Delta) that supports addcolumn
evolution. New optional columns are appended as nullable; old queries keep
working.
2.  Control schema updates:
• If using crawlers, set update behavior to “add new columns only” so it never
drops/renames.
• Prefer programmatic schema updates (Glue API / Iceberg ALTER TABLE ADD
COLUMN) at the end of the ETL that introduces the field, for deterministic changes.
3.  Shield consumers with views. Maintain consumer-facing Athena views that select only
the stable columns; update the view when stakeholders are ready to adopt new fields.
This prevents BI breakage.
4.  Validate and default. In Glue jobs, add withColumn for new fields and default to null or a
safe value when absent. Add data quality rules to alert on unexpected type changes.
Redshift side
5) Land raw to S3 first. Keep an external table (Spectrum/Iceberg) over the lake to absorb
schema drift without impacting Redshift immediately.
6) Stage before target. Load daily deltas into a Redshift staging table. When optional fields are
adopted, run ALTER TABLE ADD COLUMN (nullable) on the target and update the MERGE to
populate them. Existing queries keep working because added columns are nullable and not
referenced.
7) Consider semi-structured for fast-moving schemas. If new fields appear frequently, land
them in a SUPER column (JSON) in staging and gradually materialize important ones into typed
columns. Downstream SQL can read SUPER with PartiQL during the transition.
Governance and safety
8) Version schemas. Track a schema_version in table properties and in the curated dataset.
Communicate changes and keep a simple changelog.
9) Prevent destructive changes. Disallow drops/renames via policy; if a breaking change is
required, publish a v2 table and deprecate v1 on a schedule.
10) Monitor. Use Glue data quality checks to detect unexpected type shifts or sudden null
spikes on new fields, and alert before consumers feel it.
Result: new optional fields land safely as nullable, Catalog metadata updates automatically or
via code, consumer views remain stable, and Redshift adoption happens on your terms without
breaking existing queries.
All ri ghts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q47. Your Glue Data Catalog has thousands of tables, with duplicates from different teams. 
How would you organize it to maintain a single source of truth? 
I apply a “data product” model with clear ownership, strict naming, and controlled creation so
only one authoritative table exists per dataset and zone. Everything else either references it or
gets archived.
1.  Databases by zone and domain
Create databases like sales_raw, sales_curated, sales_analytics (one domain per set).
This prevents teams from spraying tables into random places.
2.  One dataset → one table per zone (authoritative)
Adopt a naming convention: domain_dataset_zone (for example, web_clicks_curated).
Publish a short owner/SLAs in table properties (owner, contact, refresh_cadence,
source_system). Declare this table the only source of truth for that dataset/zone.
3.  Lock down who can create/alter tables
Restrict Glue CreateTable/UpdateTable to a small “catalog-admin” role or CI pipelines.
Everyone else has read-only. All table creation goes through code review (IaC:
CloudFormation/Terraform or a small boto3 script). This alone stops most duplicates.
4.  Use Resource Links and cross-account sharing
When multiple accounts need the same table, share from a central producer account
via Lake Formation and create Resource Links in consumer accounts instead of copying
schemas. Consumers see the same table, not a fork.
5.  Crawler discipline (or avoid crawlers)
If you must use crawlers, register one crawler per dataset prefix with fixed table name,
include/exclude patterns, Recrawl = “new folders only”, Update = “update in
database”. For predictable partitions, prefer partition projection and disable frequent
crawls. Never point multiple crawlers at the same prefix.
6.  Catalog hygiene and de-dup process
Run a weekly “catalog linter” job that flags duplicates by similar names/locations and
empty/never-used tables (based on CloudTrail/Athena query logs). For duplicates: pick
a canonical table, migrate consumers, then tag others as deprecated=true and set an
auto-delete date.
7.  Versioning policy for breaking changes
When schema breaks, publish dataset_v2 and mark v1 deprecated with a sunset date.
Don’t mutate the canonical table in-place in ways that break consumers.
8.  Documentation and discovery
Maintain a simple data dictionary (could be a Glue table property + Confluence page).
Publish sample queries and owners. Analysts discover the canonical table quickly and
stop creating their own.
Result: a small set of governed, owner-backed canonical tables per dataset/zone; tight
create/alter controls; duplicates detected and retired; consumers use shared links instead of
copying schemas.
 



## Q49. If you migrate from Hive Metastore (with custom SerDes and partitions) to Glue 
Catalog, how would you make it work smoothly with Athena and Redshift? 
I start by inventorying every Hive table (formats, SerDes, partition keys, table properties), then
recreate metadata in Glue in a way that Athena and Redshift Spectrum both understand. Where
Athena can’t use a custom SerDe, I convert the data to Parquet and register a new table so
queries keep working.
Plan
1.  Catalog inventory and classification
Export Hive DDL for all databases. Classify tables:
• Standard (Parquet/ORC/CSV/JSON with common SerDes) → safe to recreate directly.
• Custom/legacy SerDes or odd storage layouts → plan to rewrite data to Parquet or
keep Spark-only access.
2.  Programmatic migration of metadata
Write a small script (boto3/Glue API) that, for each table, calls create_database and
create_table with the original columns, partition keys, StorageDescriptor
(InputFormat/OutputFormat/SerDeInfo), table properties, and S3 locations. Keep
names and database structure identical where possible to minimize code changes.
3.  Handle custom SerDes explicitly
Athena supports a limited set of SerDes. If a table uses a custom SerDe that Athena
can’t read, run a one-time (or rolling) Glue/Spark job to rewrite those datasets to
Parquet in a curated prefix with the same partitioning. Register a new Athena-facing
table over Parquet while optionally keeping a Spark-only table over the legacy layout for
backfill.
4.  Partitions at scale
If there are many partitions, avoid importing each one. Enable partition projection on
Athena/Glue for predictable keys (for example, dt, region). For moderate counts, batchcreate
partitions with CreatePartition. Add a Glue partition index if you keep millions of
partitions.
5.  Schema normalization for cross-engine compatibility
Standardize types and names: lowercase column names, replace spaces/special chars
with underscores, align DECIMAL precision/scale, timestamps in UTC. Add table
properties for classification and owner/SLAs.
6.  Permissions and governance
Put the new Catalog under Lake Formation. Grant SELECT to consumers and manage
column-level controls there. Update S3 bucket policies to allow access via LF, not
direct IAM, to keep governance centralized.
7.  Redshift integration
Create an external schema in Redshift that points to the Glue Data Catalog. For
heavy/critical datasets, consider CTAS into Redshift-managed tables or use Spectrum
against Parquet/Iceberg directly. Ensure Parquet compression (Snappy/ZSTD) and sane
file sizes so Spectrum scans are efficient.
8.  Validation and cutover
For each migrated table, run sample Athena queries (count, min/max) and a small Spark
job reading via the Glue Catalog. Compare results to Hive. Cut over consumers in
batches. Keep a rollback plan and tag old Hive entries as deprecated during transition.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 
Result: Glue mirrors Hive for supported formats, non-supported SerDes are converted to
Parquet for Athena/Spectrum, partitions remain queryable at scale, and both Athena and
Redshift read consistently through the Glue Catalog.

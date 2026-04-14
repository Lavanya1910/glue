# MODULE 9: DATA QUALITY & PREPARATION


## Q36. How would you use Glue DataBrew to clean raw customer data with nulls, bad dates, 
and extra spaces before storing curated data? 
I create a repeatable DataBrew recipe that standardizes text, fixes dates, handles nulls, and
outputs compact Parquet to a curated S3 prefix, then schedule it.
Setup
• Create a DataBrew dataset pointing to the raw S3 prefix (CSV/JSON/Parquet).
• Run a data profile to see null rates, distinct counts, outliers, and bad formats.
• Define a project with a recipe; lock column data types (strings, integers, timestamps in UTC).
Recipe steps (typical sequence)
• Trim and normalize text: Trim whitespace, collapse multiple spaces, standardize case (for
example, names to proper case, emails to lowercase), remove control characters.
• Clean dates: Parse with explicit formats (for example, dd-MM-yyyy vs MM/dd/yyyy). Coerce
invalid dates to null, enforce ranges (for example, birth_date between 1900-01-01 and today),
and standardize to ISO 8601 UTC.
• Handle nulls:
– For required business keys, drop rows with null keys to a rejection file.
– For optional fields, impute with sensible defaults (median for numeric, mode for small
categorical, or empty string) and add “_imputed” flags for transparency.
• Fix common anomalies: split combined fields (for example, “city, state”), trim leading zeros
when appropriate, remove surrounding quotes, normalize phone numbers to E.164, and
standardize country/region codes with lookup mappings.
• Deduplicate: define a key (for example, email_normalized + birth_date) and keep the most
recent record by update_ts.
• Schema enforcement: cast columns to target types, rename to snake_case, and drop unused
columns.
• Validations: add rules (for example, email must match regex, date not in future). Send failures
to a quarantine S3 path with the reason.
Output and scheduling
• Set the job to write Parquet with Snappy or ZSTD, partitioned by dt=YYYY-MM-DD, and target
256–512 MB files (DataBrew job size settings influence file count).
• Store cleansed data to s3://lake/curated/customers/ and rejected records to
s3://lake/quarantine/customers/.
• Create a schedule (hourly/daily) or trigger via EventBridge after raw drops complete.
• Optionally register the curated path in the Glue Data Catalog (crawler or manual schema) so
Athena/Glue can query it.
• Monitor runs with CloudWatch metrics and review the profile reports over time to catch drift.
Result: a click-built, versioned recipe that consistently trims spaces, fixes dates, handles nulls,
enforces schema, and lands compact, query-friendly Parquet ready for Athena or downstream
Glue jobs.
 



## Q37. How would you use Glue FindMatches to detect and merge duplicate customer 
records, and what parameters would you tune? 
I treat this as a two-part problem: first, use Glue FindMatches to generate match groups of
likely duplicates; second, apply clear survivorship rules to merge those groups into a single
“golden” customer record.
Plan
1.  Prepare input data
Clean obvious issues first so the model focuses on real duplicates: trim spaces,
standardize case, normalize phone to E.164, split full name into first/last, standardize
addresses where possible, and keep a stable primary key column like source_record_id.
Only keep columns that help match quality (name, email, phone, address, DOB, etc.).
2.  Create a FindMatches ML Transform
Point it to the prepared dataset in S3/Glue Catalog and set the primaryKeyColumnName
to source_record_id. Start with a representative sample if the table is huge.
3.  Generate and label a training set
Use the “labeling set generation” to produce candidate pairs. Manually label a few
hundred to a few thousand as “match” or “no match.” Include tricky edge cases: same
name different person, nicknames, typos, international formats.
4.  Train and evaluate
Train the transform and review precision, recall, and F1. If precision is too low (too many
false matches), tune for stricter matching; if recall is too low (missed duplicates), tune
for more permissive matching.
5.  Tune key parameters
• precisionRecallTradeoff: move toward 1.0 for higher precision (fewer false merges,
may miss some dupes), toward 0.0 for higher recall (catch more dupes, risk more false
merges).
• accuracyCostTradeoff: increase if the business prefers fewer wrong merges even at
the cost of missing some duplicates; decrease if catching all duplicates is more
important.
• enforceProvidedLabels: set true once you trust your labels so the model adheres to
them strictly.
• computeStatistics: enable to help interpret results during tuning.
I iterate: retrain, inspect example clusters, adjust tradeoffs until the confusion matrix
looks acceptable for the business risk tolerance.
6.  Run the transform on full data
The output adds a match_id (cluster/group id) and a confidence score per record. Now I
have groups of records that likely represent the same customer.
7.  Merge with survivorship rules in a Glue job
For each match_id group:
• Pick the survivorship “base” record (for example, the most recent by update_ts or the
record from the system of record).
• For each column, apply a rule: prefer non-null, prefer verified over unverified, pick
longest string for address, pick most frequent value when consistent, or use a priority
order of sources.
• Keep a crosswalk table that maps all source_record_id values to the new
golden_customer_id so downstream systems can trace lineage.
• Write the golden table to S3 (Delta/Hudi/Iceberg) and optionally publish back keys to
source systems if needed.
 
8.  Operational guardrails
Schedule periodic re-runs as new data lands, log merges for audit, and add a manual
review queue for low-confidence groups before final merge. Keep PII locked down with
Lake Formation and KMS.
Result: FindMatches gives me high-quality duplicate clusters with controllable precision/recall;
simple, transparent survivorship rules create auditable, trusted golden customer records.



## Q38. How would you apply Glue data quality checks to ensure fields like customer_id and 
order_date are always valid? 
I define a Glue Data Quality ruleset, run it automatically in pipelines, fail fast on critical checks,
and quarantine bad rows so the rest can proceed safely.
Design
1.  Start with profiling and rule discovery
Run a Glue Data Quality “recommendation” on recent partitions to auto-suggest
checks. Then convert those into explicit rules and add a few business-specific ones.
2.  Core rules for customer_id
• Completeness: customer_id is not null.
• Uniqueness: unique per table or per partition (decide the correct level).
• Format: matches expected pattern (for example, prefix + digits) or length within a
range.
• Type: strictly string or bigint (no implicit casts).
• Referential integrity: if there’s a master dimension, customer_id must exist there.
• Stability: sudden new distinct counts trigger a warning (guard against ID generator
bugs).
3.  Core rules for order_date
• Completeness: not null.
• Parseability: convertible to timestamp.
• Range validity: not in the future beyond an allowed skew (for example, ≤ 24 hours
ahead) and not older than a defined lower bound if that’s a requirement.
• Consistency: timezone normalized (UTC) and within business hours if relevant.
• Freshness: max(order_date) must be within an expected freshness window for
daily/hourly feeds.
4.  Implement rules in Glue Data Quality
Create a ruleset and attach it to the job step that validates each arriving partition before
curation. Configure thresholds: critical rules fail the job; non-critical rules log warnings.
Send metrics to CloudWatch and write a human-readable report to S3.
5.  Handle failures without blocking everything
Use two outputs:
• Pass path → curated table.
• Fail path → quarantine table with a reason column (for example,
“order_date_in_future”).
Alert on any fail rate above a small percentage and open a ticket automatically if critical
checks fail.
6.  Keep checks fast and cheap
Evaluate rules on the current partition or a recent sliding window, not the whole table.
Push filters to scan only new data. Keep schemas tight so type checks are cheap.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 
7.  Evolve the rules safely
Version rulesets. When schema changes add columns, update the ruleset and run in
“warn” mode for a short period before making them “fail” rules. Review DQ trends
weekly to adjust limits (for example, acceptable null rates).
8.  Downstream enforcement
Block publishing to Redshift/BI unless the DQ step reports success for that batch.
Stamp each curated file with dq_pass=true and a ruleset version so consumers can
trust the data.
Result: every batch is automatically checked for non-null, unique, well-formed customer IDs
and for sane, timely order dates; bad data is isolated, good data flows through, and dashboards
remain trustworthy.
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 



## Q39. If FindMatches misses true duplicates or flags false ones, how would you improve its 
precision vs recall? 
I iterate on three things: better training labels, cleaner matching features, and the model tradeoff
knobs. My goal is to align the model’s mistakes with business risk (merging two different
people is usually worse than leaving a duplicate).
1.  Improve the labels
I regenerate the labeling set and hand-label more pairs, especially edge cases
(nicknames, typos, international formats). I balance “match” and “no match” examples
and include hard negatives (same name, different person). More and better labels move
both precision and recall up.
2.  Engineer better matching signals
Before training/inference, I normalize inputs so the model compares apples to apples:
lowercase emails, E.164 phones, trimmed names, split full_name into first/last,
standardized addresses, canonical country/state codes, and UTC dates. I add phonetic
keys (Soundex/Metaphone) and nickname mappings so “Jon/John” match. Cleaner,
richer features reduce both missed matches and false matches.
3.  Tune FindMatches trade-offs
• precisionRecallTradeoff: move toward 1.0 to be stricter (higher precision, lower recall)
when false merges are costly; move toward 0.0 to catch more dupes when missing
them is costly.
• accuracyCostTradeoff: increase when you want fewer false positives even if recall
drops; decrease when you can review low-confidence matches manually.
• enforceProvidedLabels: set true once labels are trusted so the model honors them
strongly.
I retrain after each adjustment and check precision/recall/F1 and sample clusters.
4.  Adjust the decision threshold and post-processing
I set a higher minimum score for auto-merge and route mid-score pairs to a human
review queue. I also choose cautious clustering logic (avoid chaining low-confidence
edges) to prevent “match waterfalls.”
5.  Block to reduce bad comparisons
I use blocking keys (for example, same email domain or same postal code) to avoid
comparing records that cannot match. That reduces random pairings and improves
precision.
6.  Close the loop operationally
I log false merges/splits reported by users, add them to the label set, and retrain
periodically. Over time the model adapts to real data quirks.
Result: cleaner features + stronger labels + tuned trade-offs give a model that hits the
precision/recall balance the business needs, with a manual review safety net for ambiguous
cases.
 



## Q40. How would you design a workflow so only datasets with >95% data quality score are 
published to the curated zone? 
I gate publishing on a data quality step and branch the pipeline based on the computed score.
Anything below 95% goes to quarantine with alerts; only passes reach curated.
1.  Define rules and the score
I create a Glue Data Quality ruleset for the dataset (completeness, uniqueness, valid
formats, referential checks, date ranges). I enable the built-in score so each run outputs
a numeric quality score and per-rule results.
2.  Orchestrate with a gate
I run the pipeline as: ingest → validate (DQ) → branch → curate/quarantine. This can be a
single Glue job with logic, or a Glue Workflow/Step Functions state machine with a
Choice state that reads the DQ result file.
3.  Branching logic
After the DQ run, the job reads the score (for example, from the Data Quality result
JSON in S3).
• If score ≥ 0.95: write to a temporary curated path, update the Glue Catalog atomically
(or swap a table version/manifest), then mark the partition as published.
• If score < 0.95: write the batch to a quarantine path with a “reason” column, emit an
SNS/Slack alert, and open a ticket automatically.
4.  Keep it idempotent and safe
I always write to a staging location first and only “promote” to curated after the pass
condition. That avoids partial publishes. I tag partitions with dq_pass=true and store the
ruleset version alongside, so auditors and consumers know what standard was applied.
5.  Fast and cheap checks
I evaluate rules only on the new partition/window, push filters to avoid scanning history,
and keep schemas strict so type checks are cheap. Critical rules are “fail” (block
publish); informational rules are “warn” (publish but alert).
6.  Governance and observability
I publish the score and rule metrics to CloudWatch for dashboards and SLAs. I set
alarms if scores trend down or if we see repeated quarantines. Lake Formation governs
access so analysts never see quarantined data unless they have a special role.
7.  Continuous improvement
When schema evolves, I version the ruleset, run in warn-only for a few runs, then
promote to fail once stable. I review top failure reasons monthly and fix upstream issues
to keep pass rates high.
Result: only batches with a verified ≥95% quality score enter curated, promotion is atomic and
auditable, and bad data is isolated with clear reasons and alerts.
 

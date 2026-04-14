

🔥 MODULE 1: INGESTION & CATALOGING
Q1. How would you use Glue components (Jobs, Crawlers, Catalog, Triggers) to move raw data from S3 to Redshift automatically every hour?
Q2. Your S3 bucket has millions of partitioned files. How would you make Glue Crawlers faster and cheaper?
Q3. Custom Classifier If the default classifier fails on complex nested logs, how would you build a custom Glue classifier?
Q4. Duplicate Tables - A crawler created separate tables for CSV and Parquet files of the same dataset. How would you keep only one logical table?
Q5. Your lake has Parquet (historical) + JSON (incremental). How would you design crawlers/classifiers so Athena can query both together?
Q26. How would you set up Crawlers + Data Catalog so Athena can query new raw S3 datasets automatically?
Q41. If you have datasets in different formats (CSV, JSON, Parquet) across S3 buckets, how would you make them queryable in Athena while avoiding duplicate tables in Glue Catalog?
Q45. How would you let analysts query raw, curated, and aggregated S3 data in Athena without them manually creating schemas?
Q50. How would you configure a Glue Crawler to handle new fields in JSON data without breaking Athena queries?

🔥 MODULE 2: BATCH ETL DESIGN
Q6. How would you design a Glue ETL job to keep only the latest session record per user in clickstream data? Would you use DynamicFrames or DataFrames?
Q7. How would you join a small customer dataset with a huge transaction dataset in Glue efficiently?
Q9. If your S3 dataset has mismatched field names/types compared to Redshift, how would you transform it in Glue before loading?
Q10. You have deeply nested JSON with arrays/structs. How would you flatten it in Glue, and when would you prefer DynamicFrames over DataFrames?
Q21. How would you speed up a Glue job on billions of rows in S3 using partition pruning and pushdown filters?
Q27. What’s the most efficient way to load terabytes of transformed data from Glue to Redshift, and how would you handle the small file problem?
Q30. How would you join Redshift customer master data with S3 clickstream data in Glue, and what schema/performance/cost challenges might arise?

🔥 MODULE 3: INCREMENTAL PROCESSING
Q8. Your Glue job keeps reprocessing the same files into Redshift. How would you use Job Bookmarks to fix this?
Q23. How would you stop a Glue job from reprocessing all history daily when only new S3 files need to go into Redshift?
Q42. Your Glue ETL job runs daily. How would you ensure only new S3 files are processed and avoid reprocessing old data?

🔥 MODULE 4: ORCHESTRATION & WORKFLOWS
Q11. How would you orchestrate three Glue jobs (Clean → Aggregate → Load) so they run in order automatically?
Q12. If Job B fails in a workflow, how would you design retries so the pipeline doesn’t restart from scratch?
Q13. How would you trigger a Glue Workflow automatically when a new file arrives in S3?
Q14. How would you combine Glue Workflows with Step Functions to get centralized failure monitoring?
Q15. How would you configure a job to run only after multiple upstream jobs complete successfully?
Q43. You have a Glue workflow with three jobs (Clean → Aggregate → Load). How would you ensure the Load job doesn’t run if the Aggregate job fails?

🔥 MODULE 5: STREAMING ETL
Q16. How would you design a Glue Streaming Job that reads from Kinesis and writes to S3 in Parquet with low latency?
Q17. If Kafka events arrive late, how would you handle them in a Glue Streaming Job so reports stay accurate?
Q18. A Glue Streaming Job reprocesses Kafka offsets after restart, causing duplicates. How would you fix it with checkpointing?
Q19. How would you enrich Kinesis streaming data with a static S3 reference dataset in Glue?
Q20. Your Glue Streaming Job lags when processing 100k events/sec. What tuning steps would you take to scale and reduce latency?

🔥 MODULE 6: PERFORMANCE & COST OPTIMIZATION
Q21. How would you speed up a Glue job on billions of rows in S3 using partition pruning and pushdown filters?
Q22. If a Glue job fails with out-of-memory errors on joins, how would you choose the right worker type while balancing cost vs performance?
Q24. Your Glue costs doubled last month. How would you use Glue Flex jobs and tuning to cut costs while meeting SLAs?
Q25. If Athena queries on Parquet output are still slow/expensive, what Glue optimizations (e.g., partitioning, bucketing, compression) would you apply?

🔥 MODULE 7: DATA WAREHOUSE & QUERY LAYER
Q27. What’s the most efficient way to load terabytes of transformed data from Glue to Redshift, and how would you handle the small file problem?
Q28. How would you integrate Glue with Lake Formation so analysts can query only specific columns in Athena?
Q29. How would you design a pipeline where Glue writes streaming data to S3, but analysts query it almost in real time via Athena?
Q30. How would you join Redshift customer master data with S3 clickstream data in Glue, and what schema/performance/cost challenges might arise?

🔥 MODULE 8: SECURITY, GOVERNANCE & COMPLIANCE
Q31. How would you design IAM roles so a Glue job reading/writing customer PII in S3 has only minimum permissions?
Q32. How would you ensure Glue pipelines encrypt data at rest and in transit (S3, Glue, Redshift) to meet compliance?
Q33. How would you use Lake Formation to let analysts query Athena tables but hide sensitive PII columns?
Q34. If a Glue job role has S3 permissions but still can’t access a bucket, how would you troubleshoot IAM/trust issues?
Q35. How would you design Glue jobs to mask or delete personal data (PII) on request to meet GDPR requirements?
Q48. How would you use Glue + Lake Formation to give analysts access only to curated data, while engineers can access both raw and curated?

🔥 MODULE 9: DATA QUALITY & PREPARATION
Q36. How would you use Glue DataBrew to clean raw customer data with nulls, bad dates, and extra spaces before storing curated data?
Q37. How would you use Glue FindMatches to detect and merge duplicate customer records, and what parameters would you tune?
Q38. How would you apply Glue data quality checks to ensure fields like customer_id and order_date are always valid?
Q39. If FindMatches misses true duplicates or flags false ones, how would you improve its precision vs recall?
Q40. How would you design a workflow so only datasets with >95% data quality score are published to the curated zone?

🔥 MODULE 10: METADATA & SCHEMA GOVERNANCE
Q44. If you are migrating an on-prem Hive-based Spark ETL pipeline to Glue, how would you map Hive Metastore metadata into Glue Data Catalog?
Q46. If a source keeps adding new optional fields, how would you manage schema changes in Glue so Athena/Redshift queries don’t break?
Q47. Your Glue Data Catalog has thousands of tables, with duplicates from different teams. How would you organize it to maintain a single source of truth?
Q49. If you migrate from Hive Metastore (with custom SerDes and partitions) to Glue Catalog, how would you make it work smoothly with Athena and Redshift?

Now this is exactly what interview prep doc should be — clean, numbered, structured, no loss.


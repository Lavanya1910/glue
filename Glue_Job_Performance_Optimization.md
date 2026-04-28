# Glue Job Performance Optimization

Question 1

Glue Job Performance Optimization (1TB, 2hrs→30min) ▼
Scenario: Your Glue job processes 1TB of data but takes 2 hours. Business requires 30-minute SLA. Current config: 10 DPU (G.2X), single partition processing. What optimization strategies would you implement?

Solution:
Increase DPU to 100+ G.2X for parallel execution
Enable S3 intelligent tiering + CloudFront caching
Implement partition pruning with Glue partitions
Use coalesce() to reduce output file count
Switch to Spark adaptive query execution
Pre-aggregate data using Glue DataBrew
DataArchitectStud
Real-World Example (Netflix): Reduced data pipeline from 180min to 25min by migrating from EMR to Glue with 500 DPU allocation, S3 partitioning strategy, and DataFrame coalesce optimization.
Python Implementation:
# Glue Job for optimized 1TB processing
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("OptimizedGlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# Read 1TB data with partition pruning
df = spark.read.parquet("s3://bucket/data/year=2024/month=03/")
# Filter early to reduce data volume
filtered_df = df.filter(df.status == "active")
# Apply transformations
transformed_df = filtered_df.groupBy("customer_id").agg({"amount": "sum"})
# Coalesce to reduce shuffle overhead
final_df = transformed_df.coalesce(50)
# Write with partitioning
final_df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/output/"
job.commit()
Common Pitfalls:

Blindly increasing DPU without fixing skewed joins
Not considering S3 request throttling limits
Using repartition() instead of coalesce()
Ignoring Glue catalog stats for query optimization
Performance Approach: Measure baseline → Profile with Spark UI → Identify shuffle/scan bottlenecks →
Apply DPU scaling → Optimize I/O → Test SLA compliance
DataArchitectStud
Expert Tip: Use Glue job bookmarks with S3 intelligent tiering. Enable Spark AQE and monitor via CloudWatch metrics. Target 30-40 DPU per GB/sec throughput.

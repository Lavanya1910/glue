# Glue Streaming (real-time ETL)

Question 5

Glue Streaming (real-time ETL) ▼
Scenario: Real-time stream of IoT sensor data from 10,000 devices. Need to process 100K events/second, perform aggregations, detect anomalies. Architecture and implementation?

Solution:
Use Glue Streaming with Kafka/Kinesis source
Implement microbatch processing (1-5 second windows)
Use Spark structured streaming for stateful operations
Apply watermarking for late-arriving data
Write results to S3 + DynamoDB for fast lookup
Monitor checkpoint health and lag
Real-World Example (Tesla): Processes 2M+ telemetry events/sec using Glue Streaming. Detects vehicle anomalies within 50ms and triggers alerts to service centers.
Python Implementation:
# Glue Streaming for IoT data
from awsglue.context import GlueContext
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType glueContext = GlueContext(spark.sparkContext)

# Define schema
schema = StructType([
StructField("device_id", StringType()), StructField("temperature", DoubleType()), StructField("humidity", DoubleType()), StructField("timestamp", StringType()) ])

# Read from Kinesis stream df = spark.readStream \
.format("kinesis") \
chitectStud
.option("streamName", "iot-sensors") \
.option("region", "us-east-1") \
.option("initialPosition", "TRIM_HORIZON") \
.load()
# Parse JSON
parsed_df = df.select(
from_json(col("data").cast("string"), schema).alias("parsed") ).select("parsed.*")
# Time-window aggregation
windowed = parsed_df \
.withWatermark("timestamp", "10 seconds") \
.groupBy(window("timestamp", "5 seconds"), "device_id") \
.agg(
avg("temperature").alias("avg_temp"),
avg("humidity").alias("avg_humidity")
 DataAr
)
# Anomaly detection
anomalies = windowed.filter(col("avg_temp") > 50)
# Write to S3 (parquet)
query1 = parsed_df.writeStream \
.format("parquet") \
.option("path", "s3://bucket/iot-data/") \
.option("checkpointLocation", "s3://bucket/checkpoints/iot/") \.option("mergeSchema", "true") \
.start()
# Write anomalies to DynamoDB for real-time alerting
query2 = anomalies.writeStream \
.format("dynamodb") \
.option("table", "IoTAnomalies") \
.option("region", "us-east-1") \
.option("checkpointLocation", "s3://bucket/checkpoints/anomalies/") \.start()
query1.awaitTermination()
query2.awaitTermination()
Common Pitfalls:

Using batch mode for streaming workloads
Not managing state size for stateful operations
Ignoring late-arriving data (no watermarking)
High checkpoint latency causing processing lag
chitectStud
Streaming Approach: Define schema → Set microbatch interval → Implement stateful ops → Add watermarking → Monitor lag metrics
Expert Tip: Use 5-second windows for balance between latency and overhead. Monitor checkpoint duration and state size. Partition output by device_id for efficient querying.

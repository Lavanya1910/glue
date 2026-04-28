# All Questions

1. Glue Job Performance Optimization (1TB, 2hrs→30min)
   Scenario: Your Glue job processes 1TB of data but takes 2 hours. Business requires 30-minute SLA. Current config: 10 DPU (G.2X), single partition processing. What optimization strategies would you implement?

2. Glue Catalog & Metadata Management
   Scenario: Managing 500+ tables across 10 databases. Need version control, lineage tracking, and tag based access control. How would you implement this?

3. Glue Connectors (Salesforce, SAP, Kafka)
   Scenario: Integrate Salesforce, SAP, and Kafka into Glue data lake. Handling authentication, incremental sync, and schema changes. Implementation approach?

4. Error Handling & Data Quality
   Scenario: Pipeline processes financial transactions. Need robust error handling, data quality checks, and alerting for anomalies. How would you implement?

5. Glue Streaming (real-time ETL)
   Scenario: Real-time stream of IoT sensor data from 10,000 devices. Need to process 100K events/second, perform aggregations, detect anomalies. Architecture and implementation?

6. Glue Cost Optimization
   Scenario: Monthly Glue bill is $50K with 100 running jobs. Target: reduce to $20K without impacting SLA. Cost optimization strategy?

7. Glue DataBrew (data preparation)
   Scenario: Customer dataset with 40% missing values, inconsistent date formats, duplicate records. Need quick data cleaning. DataBrew approach?

8. Glue Bookmarks (incremental processing)
   Scenario: Process 1M+ new database records daily. Current full reload takes 4 hours. Need incremental processing with bookmark-based tracking. Implementation?

9. Glue DPU Scaling strategies
   Scenario: Glue workloads vary: small 10DPU jobs peak at 2PM, medium 50DPU jobs at 5PM, large 200DPU batch at 10PM. Design optimal DPU allocation strategy.

10. Glue Triggers (scheduled vs event)
   Scenario: Complex pipeline: Import → Clean → Aggregate → Report. Import can be manual or automated. Clean depends on import completion. Design trigger strategy for reliability.

11. Glue Schema Registry
   Scenario: 50+ teams producing events to Kafka. Need schema governance, compatibility checks, versioning. Schema Registry implementation?

12. Glue Partition Projection
   Scenario: Athena queries on 10 years of daily partitioned data (3,650+ partitions). Crawler adds partitions slowly. Improve query performance and partition discovery.

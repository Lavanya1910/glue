# Glue Cost Optimization

## Question 6

### Glue Cost Optimization

**Scenario:**
Monthly Glue bill is **$50K** with **100 running jobs**.

**Target:** Reduce cost to **$20K** without impacting SLA.

**Cost optimization strategy?**

---

## Solution

* Right-size DPU allocation per job (audit current usage)
* Consolidate 10–15 small jobs into a single job
* Use G.1X (cheaper) instead of G.2X where workloads are I/O limited
* Implement job failure early exits to avoid wasted DPU
* Schedule non-critical jobs during off-peak hours
* Cache intermediate results in S3 to avoid recomputation

---

### Real-World Example (Airbnb)

Reduced Glue costs by **60%** by migrating **300 jobs** to G.1X and implementing job consolidation while maintaining the same SLA.

---

## Cost Analysis & Optimization

```python
# Cost optimization analysis

import boto3
import json
from datetime import datetime, timedelta

glue_client = boto3.client('glue')
ce_client = boto3.client('ce')


# Get all jobs with metrics
def analyze_job_costs():

    jobs = glue_client.list_jobs()['JobNames']

    for job_name in jobs:

        job_detail = glue_client.get_job(
            Name=job_name
        )['Job']

        # Extract metrics
        max_capacity = job_detail.get('MaxCapacity', 0)
        glue_version = job_detail.get('GlueVersion', '3.0')
        worker_type = job_detail.get('WorkerType', 'G.2X')

        # Monthly cost calculation
        # G.2X: $0.44/DPU-hour
        # G.1X: $0.25/DPU-hour

        hourly_cost_g2x = max_capacity * 0.44
        hourly_cost_g1x = max_capacity * 0.25

        monthly_cost = hourly_cost_g2x * 168  # 730 hours/month

        print(f"Job: {job_name}")
        print(
            f"Current: {worker_type}, "
            f"{max_capacity} DPU -> ${monthly_cost:.2f}"
        )

        print(
            f"G.1X savings: "
            f"${(hourly_cost_g2x - hourly_cost_g1x) * 730:.2f}/month"
        )

        print()


# Query Cost Explorer for Glue spending
def get_glue_cost_breakdown():

    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': (
                datetime.now() - timedelta(days=30)
            ).strftime('%Y-%m-%d'),

            'End': datetime.now().strftime('%Y-%m-%d')
        },

        Granularity='DAILY',

        Filter={
            'Dimensions': {
                'Key': 'SERVICE',
                'Values': ['AWS Glue']
            }
        },

        Metrics=['UnblendedCost']
    )

    total_cost = sum(
        float(day['Total']['UnblendedCost']['Amount'])
        for day in response['ResultsByTime']
    )

    print(f"30-day Glue spending: ${total_cost:.2f}")

    return total_cost


analyze_job_costs()
get_glue_cost_breakdown()
```

---

## Common Pitfalls

* Allocating max DPU to all jobs without profiling
* Running duplicate jobs unintentionally
* Not using reserved capacity discounts
* Ignoring S3 request costs in optimization

---

## Cost Approach

**Audit current usage → Profile job requirements → Right-size DPU → Consolidate jobs → Monitor Cost Explorer**

---

## Expert Tip

Use AWS Compute Optimizer for DPU recommendations.

Analyze CloudWatch metrics before scaling.

G.1X is sufficient for most I/O-bound workloads.

Consider Glue reserved capacity for predictable workloads.

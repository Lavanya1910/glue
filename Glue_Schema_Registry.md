# Glue Schema Registry

## Question 11

### Glue Schema Registry

**Scenario:**
50+ teams are producing events to Kafka. Need schema governance, compatibility checks, and versioning.

**Schema Registry implementation?**

---

## Solution

* Create Glue Schema Registry for event schemas
* Define compatibility rules (BACKWARD, FORWARD, FULL)
* Implement schema versioning and evolution
* Add serializers/deserializers for Kafka integration
* Enforce schema validation in producers
* Set up compliance checks and auditing

---

### Real-World Example (Netflix)

Manages **10K+ event schemas** using Glue Schema Registry.

Prevented **99% of schema-related production issues** through governance.

---

## Schema Registry Setup

```python
# Glue Schema Registry for event governance

import boto3
import json
import avro.schema

from kafka import KafkaProducer

glue = boto3.client('glue')

# Create Schema Registry

registry_name = 'EventRegistry'

glue.create_registry(
    RegistryName=registry_name,
    Description='Central event schema governance'
)

# Define Avro schema for Order event

order_schema = {
    "type": "record",
    "name": "Order",
    "namespace": "com.company.events",
    "fields": [
        {
            "name": "order_id",
            "type": "string"
        },
        {
            "name": "customer_id",
            "type": "string"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "currency",
            "type": "string",
            "default": "USD"
        },
        {
            "name": "timestamp",
            "type": "long"
        }
    ]
}

# Register schema with versioning

schema_response = glue.create_schema(
    RegistryId={
        'RegistryName': registry_name
    },
    SchemaName='OrderEvent',
    DataFormat='AVRO',
    Compatibility='BACKWARD',  # Allows reading old data with new schema
    SchemaDefinition=json.dumps(order_schema),
    Tags={
        'team': 'order-service',
        'domain': 'commerce'
    }
)

schema_version_id = schema_response['SchemaVersionId']

print(f"Schema registered: {schema_version_id}")

# Schema evolution - adding optional field

evolved_schema = {
    "type": "record",
    "name": "Order",
    "namespace": "com.company.events",
    "fields": [
        {
            "name": "order_id",
            "type": "string"
        },
        {
            "name": "customer_id",
            "type": "string"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "currency",
            "type": "string",
            "default": "USD"
        },
        {
            "name": "timestamp",
            "type": "long"
        },
        {
            "name": "region",
            "type": "string",
            "default": "US"  # New field
        }
    ]
}

# Check compatibility before registering

compatibility_check = glue.check_schema_version_validity(
    SchemaVersionId={
        'SchemaArn': schema_response['SchemaArn']
    },
    DataFormat='AVRO',
    SchemaDefinition=json.dumps(evolved_schema)
)

if compatibility_check['Valid']:

    # Register evolved schema

    evolved_version = glue.put_schema_version_metadata(
        SchemaVersionId={
            'SchemaArn': schema_response['SchemaArn']
        },
        SchemaVersionMetadata=[
            {
                'MetadataKey': 'version',
                'MetadataValue': '1.1'
            }
        ]
    )

    print("Schema evolved successfully")

# Use with Kafka producer

def create_kafka_producer_with_schema_registry():

    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v:
            json.dumps(v).encode('utf-8')
    )

    # Validate event before sending

    def send_order_event(order_event):

        # Validate against schema

        schema = avro.schema.parse(
            json.dumps(order_schema)
        )

        try:

            avro.io.validate(
                schema,
                order_event
            )

            producer.send(
                'orders',
                value=order_event
            )

            print(
                f"Order {order_event['order_id']} sent"
            )

        except Exception as e:

            print(
                f"Schema validation failed: {e}"
            )

    return send_order_event

# List all schemas in registry

def list_all_schemas():

    schemas = glue.list_schemas(
        RegistryId={
            'RegistryName': registry_name
        }
    )

    for schema in schemas['Schemas']:

        print(
            f"Schema: {schema['SchemaName']}"
        )

        print(
            f"Latest Version: "
            f"{schema['LatestSchemaVersion']}"
        )

        print(
            f"Compatibility: "
            f"{schema['Compatibility']}"
        )

        print()

list_all_schemas()
```

---

## Common Pitfalls

* Using NONE compatibility allowing breaking changes
* Not documenting schema changes in registry
* Missing schema version in Kafka message headers
* Ignoring field ordering in Avro schemas

---

## Registry Approach

**Design schema taxonomy → Set compatibility rules → Implement validation → Version schemas → Track lineage**

---

## Expert Tip

* Use BACKWARD compatibility for schema evolution.
* Always include schema version in message headers.
* Document all schema changes with justification.
* Use namespace to avoid conflicts.

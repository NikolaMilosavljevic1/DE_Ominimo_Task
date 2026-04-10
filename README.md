# Ominimo - Motor Insurance Policy Ingestion Pipeline

> A metadata-driven PySpark framework for ingesting, validating, and storing motor insurance policy data. Pipeline behaviour - what to read, what to validate, and where to write - is defined entirely in a JSON config file. Changing the config changes the pipeline without touching any Python code.

> All the data used for testing inside motor_policy.json files are made up.

---

## Project Structure

```
├── config/
│   └── pipeline.json                  # Pipeline metadata - the single source of truth
├── data/
│   ├── input/events/motor_policy/     # Input JSON Lines files (one policy per line)
│   └── output/
│       ├── events/motor_policy/       # Valid records + ingestion timestamp
│       └── discards/motor_policy/     # Rejected records + validation errors
├── src/
│   ├── main.py                        # Entry point of the application
│   ├── metadata_parser.py             # Parses pipeline.json into typed config objects
│   ├── validator.py                   # Applies field-level validation rules from metadata
│   └── pipeline_runner.py             # Executes sources -> transformations -> sinks
├── tests/
│   ├── test_metadata_parser.py
│   └── test_validator.py
├── dags/
│   └── motor_ingestion_dag.py         # Airflow DAG for production scheduling (optional)
├── Dockerfile
└── docker-compose.yml
```

---

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — the only local dependency. Java, Python, and PySpark all run inside the container.

---

## How to Run

### 1. Build the image

```bash
docker compose build
```

### 2. Run the pipeline

```bash
docker compose up pipeline
```

Results are written to the `data/output/` directory on your host machine.

### 3. Run the tests

```bash
docker compose --profile testing run --rm tests
```

---

## How It Works

The pipeline executes three phases driven entirely by `config/pipeline.json`:

```
pipeline.json
     │
     ▼
[1. SOURCES]      Read all .jsonl files from the input path
     │
     ▼
[2. TRANSFORMATIONS]
     ├── validate_fields   → splits records into OK and KO streams
     └── add_fields        → adds ingestion_dt to valid records
     │
     ▼
[3. SINKS]
     ├── events/motor_policy/    ← valid records
     └── discards/motor_policy/  ← rejected records with validation_errors
```

---

## Metadata Config (`pipeline.json`)

The config declares sources, transformation rules, and output sinks. No field names, paths, or validation rules exist in the Python code.

```json
{
  "dataflows": [
    {
      "name": "motor-ingestion",
      "sources": [
        {
          "name": "policy_inputs",
          "path": "/data/input/events/motor_policy",
          "format": "JSON"
        }
      ],
      "transformations": [
        {
          "name": "validation",
          "type": "validate_fields",
          "params": {
            "input": "policy_inputs",
            "validations": [
              {
                "field": "plate_number",
                "validations": ["notEmpty", "validPlate"]
              },
              {
                "field": "driver_age",
                "validations": ["notNull", "positive", "minAge18", "maxAge100"]
              }
            ]
          }
        },
        {
          "name": "add_ingestion_date",
          "type": "add_fields",
          "params": {
            "input": "validation_ok",
            "addFields": [
              { "name": "ingestion_dt", "function": "current_timestamp" }
            ]
          }
        }
      ],
      "sinks": [
        {
          "input": "add_ingestion_date",
          "name": "raw-ok",
          "paths": ["/data/output/events/motor_policy"],
          "format": "JSON",
          "saveMode": "OVERWRITE"
        },
        {
          "input": "validation_ko",
          "name": "raw-ko",
          "paths": ["/data/output/discards/motor_policy"],
          "format": "JSON",
          "saveMode": "OVERWRITE"
        }
      ]
    }
  ]
}
```

To add a new validation rule to a field, add its name to the `validations` array. To change input/output paths, update `path` or `paths`. No Python changes needed.

---

## Available Validation Rules

| Rule              | Applies To | Description                                                           |
| ----------------- | ---------- | --------------------------------------------------------------------- |
| `notNull`         | any        | Field must be present and non-null                                    |
| `notEmpty`        | string     | Field must be non-null and non-empty (whitespace-only fails)          |
| `positive`        | number     | Field must be greater than zero                                       |
| `minAge18`        | number     | Value must be >= 18 (legal driving age)                               |
| `maxAge100`       | number     | Value must be <=> 100                                                 |
| `validPlate`      | string     | Must match `ABC-123` format (2–3 uppercase letters, hyphen, 3 digits) |
| `validYear`       | number     | Must be between 1886 (first automobile) and the current year          |
| `validPrice`      | number     | Must be between 1.0 and 1,000,000                                     |
| `validCoverage`   | string     | Must be one of: `MTPL`, `Limited Casco`, `Casco`                      |
| `validVariant`    | string     | Must be one of: `Compact`, `Basic`, `Comfort`, `Premium`              |
| `validDeductible` | number     | Must be one of: `100`, `200`, `500`                                   |
| `validMake`       | string     | Must contain only letters, spaces, or hyphens                         |
| `validDateFormat` | string     | Must be in `yyyy-MM-dd HH:mm:ss` format                               |
| `startBeforeEnd`  | string     | `policy_start_date` must be earlier than `policy_end_date`            |

Adding a new rule means adding one entry to `VALIDATION_REGISTRY` in `validator.py` — it then becomes available to any pipeline config immediately.

---

## Input Format

JSON Lines — one policy record per line:

```json
{
  "policy_number": "54321",
  "driver_age": 30,
  "plate_number": "XYZ-789",
  "vehicle_make": "Volkswagen",
  "vehicle_year": 2019,
  "price": 480.0,
  "coverage_type": "MTPL",
  "variant": "Basic",
  "deductible": 500,
  "policy_start_date": "2024-06-01 00:00:00",
  "policy_end_date": "2025-06-01 00:00:00"
}
```

---

## Output Format

### Valid record (`events/motor_policy/`)

Passes all validation rules. An `ingestion_dt` timestamp is added.

```json
{
  "policy_number": "54321",
  "driver_age": 30,
  "plate_number": "XYZ-789",
  "vehicle_make": "Volkswagen",
  "vehicle_year": 2019,
  "price": 480.0,
  "coverage_type": "MTPL",
  "variant": "Basic",
  "deductible": 500,
  "policy_start_date": "2024-06-01 00:00:00",
  "policy_end_date": "2025-06-01 00:00:00",
  "ingestion_dt": "2026-04-10 21:30:00"
}
```

### Rejected record (`discards/motor_policy/`)

Failed at least one rule. A `validation_errors` map is added describing every failure.

```json
{
  "policy_number": "12345",
  "driver_age": 45,
  "plate_number": "",
  "ingestion_dt": "2026-04-10 21:30:00",
  "validation_errors": {
    "plate_number.notEmpty": "Field 'plate_number' failed rule 'notEmpty'",
    "plate_number.validPlate": "Field 'plate_number' failed rule 'validPlate'"
  }
}
```

---

## Orchestration (Optional)

`dags/motor_ingestion_dag.py` contains an Airflow DAG that schedules the pipeline daily at 06:00 UTC using a `DockerOperator`. It runs the same container image used locally, demonstrating how the pipeline would operate in a production environment without any code changes.

---

## Timezone

The `ingestion_dt` timestamp reflects the local time of the machine running the pipeline. The container timezone is configured via the `TZ` environment variable in `docker-compose.yml` (defaults to `Europe/Belgrade`). To override it for your timezone:

```bash
TZ=Europe/Amsterdam docker compose up pipeline
```

---

## Extending the Framework

Because sink behaviour is driven by the `format` field in `pipeline.json`, adding new output targets only requires adding a handler in `_write_sink` in `pipeline_runner.py` — the config structure stays the same.

Some natural extensions for a production environment:

- **Cloud object storage** (S3, Azure Data Lake, GCS) - Spark supports these natively, the sink path would just change to `s3://bucket/path` and credentials would be passed via environment variables
- **MongoDB** — valid records could be written to and read by a MongoDB collections using the [MongoDB Spark Connector](https://www.mongodb.com/docs/spark-connector/current/), useful if it would be needed to query individual policies by ID for example
- **Delta Lake** — already supported as a format option (`"format": "DELTA"`), adds ACID transactions and time travel on top of Parquet, which is the standard for most modern insurance data platforms
- **Apache Kafka** — rejected records could be published to a dead-letter topic instead of a local folder, allowing upstream systems to react to validation failures in real time

---

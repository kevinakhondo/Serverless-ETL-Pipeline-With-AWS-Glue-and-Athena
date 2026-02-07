# Serverless-ETL-Pipeline-With-AWS-Glue-and-Athena
This is an automated data lake pipeline that ingests raw data, transforms it, and makes it queryable.

## Key Components
- S3 data Lake with raw, processed and curated layers.
- AWS Glue crawler for schema discovery.
- AWS Glue ETL job (Python/PySpark) for transformation.
- Athena for SQL Analytics.
- EventBridge or Lambda trigger for automation

## Project Structure

```
glue-etl-pipeline/
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── s3.tf
│   ├── glue.tf
│   ├── iam.tf
│   └── athena.tf
├── scripts/
│   ├── glue_etl_job.py
│   └── generate_sample_data.py
└── README.md
```









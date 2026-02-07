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
## Hands On
### Set Up
We start by creating our work folder.

```
mkdir -p glue-etl-pipeline/{terraform,scripts}
cd glue-etl-pipeline
```
### Terraform Configuration Files
#### File 1: Terraform/variables.tf
Navigate into the terraform subfolder on your VS studio and create _variables.tf_ file. Add the following:

```
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "ecommerce-data-lake"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "DataLake"
    ManagedBy   = "Terraform"
    Environment = "dev"
  }
}
```
#### File 2: Terraform/main.tf
Create a new file called _main.tf_ and add the following:

```
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = var.tags
  }
}

# Generate unique bucket suffix
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  bucket_name = "${var.project_name}-${var.environment}-${random_id.bucket_suffix.hex}"
  athena_results_bucket = "${var.project_name}-athena-results-${var.environment}-${random_id.bucket_suffix.hex}"
}
```

#### File 3: Terraform/s3.tf
We create our S3 data lake. We shall enable server-side encryption and bucket versioning.

```
# Main data lake bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Create folder structure
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "raw/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "processed_folder" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "processed/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "curated_folder" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "curated/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "scripts_folder" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/"
  content_type = "application/x-directory"
}

# Upload Glue ETL script
resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_etl_job.py"
  source = "${path.module}/../scripts/glue_etl_job.py"
  etag   = filemd5("${path.module}/../scripts/glue_etl_job.py")
}

# Athena results bucket
resource "aws_s3_bucket" "athena_results" {
  bucket = local.athena_results_bucket
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "delete-old-queries"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}
```
#### File 4: Terraform/iam.tf
We create a service role and attach a policy.

```
# Glue service role
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn
        ]
      }
    ]
  })
}

# CloudWatch Logs policy
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name = "${var.project_name}-glue-cloudwatch-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}
```

#### File 5: Terraform/glue.tf

```
# Glue Database
resource "aws_glue_catalog_database" "data_lake_db" {
  name        = "${var.project_name}_database"
  description = "E-commerce data lake database"
}

# Glue Crawler for Raw Data
resource "aws_glue_crawler" "raw_crawler" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.data_lake_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/raw/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# Glue Crawler for Processed Data
resource "aws_glue_crawler" "processed_crawler" {
  name          = "${var.project_name}-processed-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.data_lake_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/processed/transactions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# Glue ETL Job
resource "aws_glue_job" "etl_job" {
  name     = "${var.project_name}-etl-job"
  role_arn = aws_iam_role.glue_service_role.arn
  
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/glue_etl_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SOURCE_BUCKET"                    = aws_s3_bucket.data_lake.id
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.id
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.id}/temporary/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  timeout = 60 # minutes
}

# Glue Workflow
resource "aws_glue_workflow" "etl_workflow" {
  name        = "${var.project_name}-workflow"
  description = "End-to-end ETL workflow"
}

# Trigger: Start raw crawler on-demand
resource "aws_glue_trigger" "start_raw_crawler" {
  name          = "${var.project_name}-start-raw-crawler"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl_workflow.name

  actions {
    crawler_name = aws_glue_crawler.raw_crawler.name
  }
}

# Trigger: Run ETL job after raw crawler succeeds
resource "aws_glue_trigger" "run_etl_job" {
  name          = "${var.project_name}-run-etl-job"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.raw_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.etl_job.name
  }

  start_on_creation = true
}

# Trigger: Catalog processed data after ETL job succeeds
resource "aws_glue_trigger" "catalog_processed_data" {
  name          = "${var.project_name}-catalog-processed"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.etl_job.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.processed_crawler.name
  }

  start_on_creation = true
}
```





























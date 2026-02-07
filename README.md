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
``















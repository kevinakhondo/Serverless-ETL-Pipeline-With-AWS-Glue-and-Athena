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

#### File 6: Terraform/athena.tf

```
# Athena Workgroup
resource "aws_athena_workgroup" "data_lake" {
  name        = "${var.project_name}-workgroup"
  description = "Workgroup for data lake queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/output/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}

# Named queries for common analytics
resource "aws_athena_named_query" "sales_by_region" {
  name        = "sales_by_region"
  workgroup   = aws_athena_workgroup.data_lake.id
  database    = aws_glue_catalog_database.data_lake_db.name
  description = "Analyze sales by region"

  query = <<-EOQ
    SELECT 
      region,
      COUNT(*) as transaction_count,
      SUM(total_amount) as total_sales,
      ROUND(AVG(total_amount), 2) as avg_order_value
    FROM transactions
    GROUP BY region
    ORDER BY total_sales DESC;
  EOQ
}

resource "aws_athena_named_query" "top_products" {
  name        = "top_products"
  workgroup   = aws_athena_workgroup.data_lake.id
  database    = aws_glue_catalog_database.data_lake_db.name
  description = "Top performing products"

  query = <<-EOQ
    SELECT 
      product,
      category,
      COUNT(*) as orders,
      SUM(quantity) as units_sold,
      ROUND(SUM(total_amount), 2) as revenue
    FROM transactions
    GROUP BY product, category
    ORDER BY revenue DESC
    LIMIT 10;
  EOQ
}

resource "aws_athena_named_query" "monthly_trends" {
  name        = "monthly_trends"
  workgroup   = aws_athena_workgroup.data_lake.id
  database    = aws_glue_catalog_database.data_lake_db.name
  description = "Monthly revenue trends"

  query = <<-EOQ
    SELECT 
      year,
      month,
      COUNT(*) as transactions,
      ROUND(SUM(total_amount), 2) as monthly_revenue
    FROM transactions
    GROUP BY year, month
    ORDER BY year, month;
  EOQ
}
```

#### File 7: Terraform/outputs

```
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "athena_results_bucket" {
  description = "Name of the Athena results bucket"
  value       = aws_s3_bucket.athena_results.id
}

output "glue_database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.data_lake_db.name
}

output "glue_workflow_name" {
  description = "Name of the Glue workflow"
  value       = aws_glue_workflow.etl_workflow.name
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.data_lake.id
}

output "raw_data_path" {
  description = "S3 path for raw data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/raw/"
}

output "processed_data_path" {
  description = "S3 path for processed data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/processed/"
}

output "next_steps" {
  description = "Next steps to complete the setup"
  value = <<-EOT
    1. Upload sample data:
       aws s3 cp transactions.csv s3://${aws_s3_bucket.data_lake.id}/raw/year=2024/month=01/
    
    2. Start the workflow:
       aws glue start-workflow-run --name ${aws_glue_workflow.etl_workflow.name}
    
    3. Query in Athena:
       - Open Athena console
       - Select workgroup: ${aws_athena_workgroup.data_lake.id}
       - Use database: ${aws_glue_catalog_database.data_lake_db.name}
       - Run saved queries or create your own
  EOT
}
```
### Creating the Glue ETL Job Scripts
#### File 1: scripts/glue_etl_job.py

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'TARGET_BUCKET'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting ETL job: {args['JOB_NAME']}")
print(f"Source bucket: {args['SOURCE_BUCKET']}")
print(f"Target bucket: {args['TARGET_BUCKET']}")

# Read from Glue Catalog (table created by raw crawler)
print("Reading data from Glue Catalog...")
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_data_lake_database",
    table_name="raw",
    transformation_ctx="datasource"
)

# Convert to Spark DataFrame for transformations
df = datasource.toDF()

print(f"Raw data count: {df.count()}")

# Data cleaning and transformations
print("Applying transformations...")
df_transformed = df \
    .withColumn('timestamp', to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('total_amount', round(col('quantity') * col('price'), 2)) \
    .withColumn('processing_date', current_timestamp()) \
    .filter(col('quantity') > 0) \
    .filter(col('price') > 0) \
    .filter(col('transaction_id').isNotNull()) \
    .dropDuplicates(['transaction_id'])

# Add derived columns for partitioning and analysis
df_final = df_transformed \
    .withColumn('year', year('timestamp')) \
    .withColumn('month', month('timestamp')) \
    .withColumn('day', dayofmonth('timestamp')) \
    .withColumn('day_of_week', dayofweek('timestamp')) \
    .withColumn('hour', hour('timestamp'))

print(f"Transformed data count: {df_final.count()}")

# Convert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df_final, glueContext, "dynamic_frame")

# Write to S3 in Parquet format with partitioning
print("Writing processed data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": f"s3://{args['TARGET_BUCKET']}/processed/transactions/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet",
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="datasink"
)

print("ETL job completed successfully!")
job.commit()
```
#### File 2: generate_sample_data.py

```
import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_TRANSACTIONS = 1000
OUTPUT_FILE = 'transactions.csv'

# Sample data
products = [
    'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 
    'Webcam', 'USB Cable', 'Charger', 'SSD Drive', 'RAM'
]
categories = ['Electronics', 'Accessories', 'Peripherals', 'Storage']
regions = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'ap-northeast']

# Generate transactions
transactions = []
start_date = datetime(2024, 1, 1)

for i in range(NUM_TRANSACTIONS):
    date = start_date + timedelta(
        days=random.randint(0, 60),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    transaction = {
        'transaction_id': f'TXN{i:06d}',
        'timestamp': date.strftime('%Y-%m-%d %H:%M:%S'),
        'product': random.choice(products),
        'category': random.choice(categories),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 1000), 2),
        'region': random.choice(regions),
        'customer_id': f'CUST{random.randint(1, 200):05d}'
    }
    transactions.append(transaction)

# Write to CSV
with open(OUTPUT_FILE, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=transactions[0].keys())
    writer.writeheader()
    writer.writerows(transactions)

print(f" Generated {len(transactions)} transactions in {OUTPUT_FILE}")
print(f"Date range: {transactions[0]['timestamp']} to {transactions[-1]['timestamp']}")
```

### Deploy With Terraform

```
# Navigate to terraform directory
cd terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply the configuration
terraform apply

# Type 'yes' when prompted
```
#### Results of Deployment
<img width="1680" height="670" alt="image" src="https://github.com/user-attachments/assets/5cd96fa5-bc56-434d-92fc-36d9bddaeb5a" />

<img width="1680" height="670" alt="image" src="https://github.com/user-attachments/assets/aa0ebae5-5824-4375-8763-3770ca5bb6e6" />

### Generate and Upload Sample Data
#### Generate Sample Data
Run the following scripts:

```
cd ~/glue-etl-pipeline/scripts

# Run the Python script
python3 generate_sample_data.py
```
The output is:
<img width="624" height="59" alt="image" src="https://github.com/user-attachments/assets/cb0fb291-068b-4997-9cf7-3fe988fa54f0" />

Verify that the files exists:

```
ls -lh transactions.csv
# Preview the first few lines
head -5 transactions.csv
```
The output is:

<img width="624" height="115" alt="image" src="https://github.com/user-attachments/assets/16cafdd7-300e-4782-909e-9b30ea3575eb" />

#### Upload the data to S3

```
# Get bucket name from Terraform
cd ~/glue-etl-pipeline/terraform
BUCKET_NAME=$(terraform output -raw s3_bucket_name)

echo "Bucket name: $BUCKET_NAME"

# Upload CSV to raw folder
cd ~/glue-etl-pipeline/scripts
aws s3 cp transactions.csv s3://${BUCKET_NAME}/raw/transactions.csv
```
The output is
<img width="792" height="257" alt="image" src="https://github.com/user-attachments/assets/c34ea2ae-1c2c-42ef-86dd-4d6388eb2733" />

### Run the ETL Pipeline
#### Start the workflow

```
# Get workflow name
WORKFLOW_NAME=$(cd ~/glue-etl-pipeline/terraform && terraform output -raw glue_workflow_name)

echo "Starting workflow: $WORKFLOW_NAME"

# Trigger the workflow
aws glue start-workflow-run --name ${WORKFLOW_NAME}
```
<img width="979" height="103" alt="image" src="https://github.com/user-attachments/assets/12caa790-a905-4d91-92ce-0b8d3aa3d20a" />

#### Monitor Progress

```
# Get the run ID
RUN_ID=$(aws glue get-workflow-runs --name ${WORKFLOW_NAME} --max-results 1 --query 'Runs[0].WorkflowRunId' --output text)

echo "Workflow Run ID: $RUN_ID"

# Check status
aws glue get-workflow-run \
  --name ${WORKFLOW_NAME} \
  --run-id ${RUN_ID} \
  --query 'Run.{Status:Status,StartedOn:StartedOn}' \
  --output table
```
<img width="1151" height="221" alt="image" src="https://github.com/user-attachments/assets/fbb12a52-1cd0-40f2-afd1-ed2471413b81" />

The workflow does the following:
 - Run raw crawler (discovers CSV schema, creates "raw" table)
 - Run ETL job (transforms CSV → Parquet)
 - Run processed crawler (catalogs Parquet files


Keep Checking the status

```
# Run this every minute
watch -n 60 "aws glue get-workflow-run --name ${WORKFLOW_NAME} --run-id ${RUN_ID} --query 'Run.Status' --output text"
```

#### Verification

Verify if raw crawler succeeded:

```
aws glue get-crawler --name ecommerce-data-lake-raw-crawler --query 'Crawler.LastCrawl.Status'
```
<img width="1151" height="37" alt="image" src="https://github.com/user-attachments/assets/207da918-728d-4653-ac05-3510c8c5de66" />

verify if raw table was created:

```
aws glue get-tables --database-name ecommerce-data-lake_database --query 'TableList[*].Name'

```
<img width="1151" height="77" alt="image" src="https://github.com/user-attachments/assets/9d0d7d9c-c8bc-490f-8cea-8ee8629906f9" />

Verify if the ETL job succeeded:

```
aws glue get-job-runs --job-name ecommerce-data-lake-etl-job --max-results 1 --query 'JobRuns[0].JobRunState'
# Should return: "SUCCEEDED"
```
<img width="1151" height="34" alt="image" src="https://github.com/user-attachments/assets/cca56871-3b2a-48fb-b5f0-838d9c83ae9f" />

Check if Proceed Files Exits

```
aws s3 ls s3://${BUCKET_NAME}/processed/transactions/ --recursive
# Should show Parquet files like:
# processed/transactions/year=2024/month=1/run-xxx.snappy.parquet
# processed/transactions/year=2024/month=2/run-xxx.snappy.parquet
```
<img width="1151" height="206" alt="image" src="https://github.com/user-attachments/assets/94465297-4824-4d5b-88ea-14386757e8a3" />











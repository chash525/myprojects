# 🧩 AWS Serverless Data Pipeline

This project deploys a **serverless data processing pipeline** built on AWS.  
It ingests order data, validates it against reference data, transforms it, and stores metadata in DynamoDB — all automated through AWS Step Functions and EventBridge.

---

## 🚀 Features

- **S3 Buckets** for input, processing, and output data  
- **Lambda Functions** for validation (`validate_data.py`) and transformation (`transform_data.py`)  
- **Step Functions** for workflow orchestration  
- **DynamoDB** for metadata storage  
- **SNS** for success/failure notifications  
- **SQS + EventBridge** for decoupled and scheduled triggers  
- **Encryption, Error Handling, and Cleanup** included  

---

## 🧰 Prerequisites

Before you begin, make sure you have:

- **Python 3.11+**
- **AWS CLI** configured (`aws configure sso`)
- **IAM permissions** to create S3, Lambda, Step Functions, SNS, SQS, EventBridge, and DynamoDB resources
- **IAM permissions Notes** For IAM permissions I had created new roles for this deployment to work.
---

## ⚙️ Setup Instructions

### 1 Clone repository and cd into folder
git clone ssh://git@oxfordssh.awsdev.infor.com:7999/chas.holloway/awssdk.git
cd data-pipeline
### 2 Create virtual environment and adjust email

create a virtual python environment on your local machine - python -m venv testenv
activate - source testenv/bin/activate
install - pip install -r requirements.txt Note: you may have to connect to the dsa-m3 account to gather these.
update deploy script - you will also need to input your email to receive email notifications from SNS

### 3 Deploy Pipeline

deploy - python deploy_pipeline.py

### 4 Upload Sample data
sample data for testing:
aws s3 cp sample_data/customers.json s3://m3-pipeline-processing/
aws s3 cp sample_data/product_catalog.csv s3://m3-pipeline-processing/
aws s3 cp sample_data/customer_orders_mixed.jsonl s3://m3-pipeline-input/

### Explanation of the flow
Processed metadata will appear in DynamoDB (m3-pipeline-metadata-db)

EventBridge triggers the Step Function.

Step Function invokes:

validate_data.py → checks references & validates IDs

transform_data.py → enriches data and stores results in DynamoDB

SNS notifies you of success or failure.

### Last step
cleanup - python cleanup_pipeline.py
manually clean up roles. Noted in another document.
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

# Setup

--- I had this working on another AWS environment but, currently trying to test on my own. Will come with instructions for how to set up on different AWS setups.
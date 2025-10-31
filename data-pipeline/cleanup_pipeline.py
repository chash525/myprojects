import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-1"


def delete_s3_bucket(bucket_name):
    s3 = boto3.resource("s3", region_name=REGION)
    bucket = s3.Bucket(bucket_name)
    try:
        bucket.objects.all().delete()
        bucket.delete()
        print(f"Deleted S3 bucket: {bucket_name}")
    except ClientError as e:
        print(f"Error deleting bucket {bucket_name}: {e}")


def delete_lambda_function(name):
    client = boto3.client("lambda", region_name=REGION)
    try:
        client.delete_function(FunctionName=name)
        print(f"Deleted Lambda: {name}")
    except ClientError as e:
        print(f"Error deleting Lambda {name}: {e}")


def delete_sqs_queue(queue_url):
    client = boto3.client("sqs", region_name=REGION)
    try:
        client.delete_queue(QueueUrl=queue_url)
        print(f"Deleted SQS queue: {queue_url}")
    except ClientError as e:
        print(f"Error deleting queue {queue_url}: {e}")


def delete_sns_topic(topic_arn):
    client = boto3.client("sns", region_name=REGION)
    try:
        client.delete_topic(TopicArn=topic_arn)
        print(f"Deleted SNS topic: {topic_arn}")
    except ClientError as e:
        print(f"Error deleting topic {topic_arn}: {e}")


def delete_dynamodb_table(name):
    client = boto3.client("dynamodb", region_name=REGION)
    try:
        client.delete_table(TableName=name)
        print(f"Deleted DynamoDB table: {name}")
    except ClientError as e:
        print(f"Error deleting table {name}: {e}")


def delete_step_function(arn):
    client = boto3.client("stepfunctions", region_name=REGION)
    try:
        client.delete_state_machine(stateMachineArn=arn)
        print(f"Deleted Step Function: {arn}")
    except ClientError as e:
        print(f"Error deleting Step Function {arn}: {e}")


def main():
    with open("resources_manifest.json") as f:
        resources = json.load(f)

    for bucket in resources.get("S3Buckets", []):
        delete_s3_bucket(bucket)
    for fn in resources.get("LambdaFunctions", []):
        delete_lambda_function(fn)
    for queue in resources.get("SQSQueues", []):
        delete_sqs_queue(queue)
    for topic in resources.get("SNSTopics", []):
        delete_sns_topic(topic)
    for table in resources.get("DynamoDBTables", []):
        delete_dynamodb_table(table)
    for sm in resources.get("StepFunctions", []):
        delete_step_function(sm)
    print("Cleanup complete.")


if __name__ == "__main__":
    main()

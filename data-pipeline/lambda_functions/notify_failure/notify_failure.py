import boto3
import os

sns = boto3.client("sns")

def lambda_handler(event, context):
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    message = f"Pipeline failed. Event details:\n{event}"
    
    sns.publish(
        TopicArn=topic_arn,
        Subject="M3 Pipeline Failure Notification",
        Message=message
    )
    
    return {"status": "notification_sent"}
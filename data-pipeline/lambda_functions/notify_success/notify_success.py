import boto3
import os

sns = boto3.client("sns")

def lambda_handler(event, context):
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    message = f"Pipeline succeeded! Event details:\n{event}"
    
    sns.publish(
        TopicArn=topic_arn,
        Subject="Pipeline Success Notification",
        Message=message
    )
    
    return {"status": "notification_sent"}
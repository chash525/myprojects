import boto3
from botocore.exceptions import ClientError
import json

profile_name = "my-sso-profile"
region = "us-east-1"


def get_boto3_session(profile_name=profile_name, region=region):
    try:
        session = boto3.session.Session(profile_name=profile_name, region_name=region)
        return session
    except Exception as e:
        print(f"Failed to create boto3 session: {e}")
        return None


session = get_boto3_session()


def create_s3_bucket(bucket_name, region=region):
    s3_client = session.client("s3")
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created S3 bucket: {bucket_name}")
        return True
    except ClientError as e:
        print(f"Error creating S3 bucket '{bucket_name}': {e}")
        return False


def create_sns_topic(topic_name, region=region):
    sns_client = session.client("sns")
    try:
        response = sns_client.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint="aws.testing0101@gmail.com",
        )
        print(f"Created SNS topic: {topic_name} ({topic_arn})")
        return topic_arn
    except ClientError as e:
        print(f"Error creating SNS topic: {e}")
        return None


def create_sqs_queue(sqs_name, region=region):
    sqs = session.client("sqs")
    try:
        response = sqs.create_queue(QueueName=sqs_name)
        queue_url = response["QueueUrl"]
        print(f"Created SQS queue: {sqs_name}")
        return queue_url
    except ClientError as e:
        print(f"Error creating queue '{sqs_name}': {e}")
        return None


def create_dynamodb_table(table_name, region=region):
    dynamodb = session.client("dynamodb")
    try:
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "record_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "record_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.get_waiter("table_exists").wait(TableName=table_name)
        print(f"Created DynamoDB table: {table_name}")
        return response["TableDescription"]["TableArn"]
    except ClientError as e:
        print(f"Error creating DynamoDB table '{table_name}': {e}")
        return None


def create_lambda_function(
    function_name,
    role_arn,
    zip_file_path,
    handler,
    runtime="python3.11",
    region=region,
    env_vars=None,
):
    lambda_client = session.client("lambda")
    with open(zip_file_path, "rb") as f:
        code_bytes = f.read()

    kwargs = {
        "FunctionName": function_name,
        "Runtime": runtime,
        "Role": role_arn,
        "Handler": handler,
        "Code": {"ZipFile": code_bytes},
        "Timeout": 60,
        "MemorySize": 128,
        "Publish": True,
    }

    if env_vars:
        kwargs["Environment"] = {"Variables": env_vars}

    response = lambda_client.create_function(**kwargs)
    print(f"Created Lambda function: {function_name}")
    return response["FunctionArn"]


def create_step_function(name, definition_file, role_arn, region=region):
    sfn_client = session.client("stepfunctions")
    with open(definition_file, "r") as f:
        definition = f.read()

    try:
        response = sfn_client.create_state_machine(
            name=name, definition=definition, roleArn=role_arn, type="STANDARD"
        )
        print(f"Created Step Function: {name}")
        return response["stateMachineArn"]
    except ClientError as e:
        print(f"Error creating Step Function '{name}': {e}")
        return None


def create_eventbridge_rule_for_s3(bucket_name, state_machine_arn, region=region):
    events_client = boto3.client("events", region_name=region)

    rule_name = f"{bucket_name}-trigger-stepfunction"
    event_pattern = {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail": {"bucket": {"name": [bucket_name]}},
    }

    response = events_client.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(event_pattern),
        State="ENABLED",
        Description=f"Trigger Step Function when new file lands in {bucket_name}",
    )

    rule_arn = response["RuleArn"]

    events_client.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "StepFunctionTarget",
                "Arn": state_machine_arn,
                "RoleArn": "arn:aws:iam::013929207353:role/EventBridgeStepFunctionInvokeRole",
            }
        ],
    )

    print(f"EventBridge rule created: {rule_name}")
    return rule_arn


def allow_eventbridge_to_trigger_stepfunction(state_machine_arn):
    sf_client = session.client("stepfunctions")
    statement = {
        "Sid": "AllowEventBridgeInvoke",
        "Effect": "Allow",
        "Principal": {"Service": "events.amazonaws.com"},
        "Action": "states:StartExecution",
        "Resource": state_machine_arn,
    }


def deploy_pipeline():

    input_bucket = "customer-data-pipeline-input"
    processing_bucket = "customer-data-pipeline-processing"
    output_bucket = "customer-data-pipeline-output"

    create_s3_bucket(input_bucket)
    create_s3_bucket(processing_bucket)
    create_s3_bucket(output_bucket)

    s3_client = session.client("s3")
    s3_client.put_bucket_notification_configuration(
        Bucket=input_bucket, NotificationConfiguration={"EventBridgeConfiguration": {}}
    )

    metadata_table = create_dynamodb_table("customer-data-pipeline-metadata-db")
    notifications_topic = create_sns_topic("customer-data-pipeline-notifications")
    queue_url = create_sqs_queue("customer-data-pipeline-queue")

    lambda_role_arn = ""

    validate_lambda_arn = create_lambda_function(
        function_name="customer-data-validate-data",
        role_arn=lambda_role_arn,
        zip_file_path="./lambda_functions/validate_data/validate_data.zip",
        handler="validate_data.lambda_handler",
        region=region,
        env_vars={"PROCESSING_BUCKET": processing_bucket},
    )

    transform_lambda_arn = create_lambda_function(
        function_name="customer-data-transform-data",
        role_arn=lambda_role_arn,
        zip_file_path="./lambda_functions/transform_data/transform_data.zip",
        handler="transform_data.lambda_handler",
        region=region,
        env_vars={
            "PROCESSING_BUCKET": processing_bucket,
            "OUTPUT_BUCKET": output_bucket,
        },
    )

    notify_lambda_arn = create_lambda_function(
        function_name="customer-data-notify-failure",
        role_arn=lambda_role_arn,
        zip_file_path="./lambda_functions/notify_failure/notify_failure.zip",
        handler="notify_failure.lambda_handler",
        region=region,
        env_vars={"SNS_TOPIC_ARN": notifications_topic},
    )

    notify_lambda_arn = create_lambda_function(
        function_name="customer-data-notify-success",
        role_arn=lambda_role_arn,
        zip_file_path="./lambda_functions/notify_success/notify_success.zip",
        handler="notify_success.lambda_handler",
        region=region,
        env_vars={"SNS_TOPIC_ARN": notifications_topic},
    )

    sfn_role_arn = "arn:aws:iam::659558029763:role/m3-stepfunction-role"
    step_function_arn = create_step_function(
        name="m3-pipeline",
        definition_file="./step_functions/pipeline_definition.asl.json",
        role_arn=sfn_role_arn,
        region=region,
    )

    allow_eventbridge_to_trigger_stepfunction(step_function_arn)

    event_rule_arn = create_eventbridge_rule_for_s3(
        bucket_name=input_bucket,
        state_machine_arn=step_function_arn,
        region=region,
    )

    print("Deployment complete")


if __name__ == "__main__":
    deploy_pipeline()

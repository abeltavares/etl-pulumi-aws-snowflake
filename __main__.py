import pulumi
import pulumi_aws as aws
from modules.s3 import setup_s3_buckets
from modules.lambdas import (
    setup_lambda_roles_and_policies,
    create_lambda_function,
    upload_lambda_code,
)
from modules.glue import (
    setup_crawler,
    setup_database,
    setup_job,
    upload_glue_code,
)
from modules.snowflake import setup_snowflake_resources


data_lake_bucket, output_bucket, script_buckets = setup_s3_buckets()

# Setup SQS and S3 notification
# sqs_queue = setup_sqs_and_s3_notification(data_lake_bucket)

# create_s3_event_source(lambda_handler, data_lake_bucket.bucket)

# Setting up AWS Glue resources
glue_code = upload_glue_code(script_buckets.bucket, "glue/glue_job.py")

glue_database = setup_database()
crawler = setup_crawler(data_lake_bucket.bucket, glue_database)
glue_job = setup_job(
    data_lake_bucket.bucket, output_bucket.bucket, script_buckets.bucket, glue_code.key
)

# Setting up Lambda resources
lambda_code = upload_lambda_code(script_buckets.bucket, "lambda/lambda_deployment.zip")

lambda_role = setup_lambda_roles_and_policies(
    glue_job.name, crawler.name, data_lake_bucket.bucket
)

lambda_handler = create_lambda_function(
    "myLambdaHandler",
    lambda_role.arn,
    "lambda_trigger_glue.handler",
    lambda_code.bucket,
    lambda_code.key,
    crawler.name,
    glue_job.name,
    runtime="python3.8",
)

lambda_permission = aws.lambda_.Permission(
    "lambdaPermission",
    action="lambda:InvokeFunction",
    function=lambda_handler.arn,
    principal="s3.amazonaws.com",
    source_arn=data_lake_bucket.bucket.apply(lambda bucket: f"arn:aws:s3:::{bucket}"),
    source_account=pulumi.Config().require("aws_account_id"),
)

bucket_notification = aws.s3.BucketNotification(
    "bucketNotification",
    bucket=data_lake_bucket.bucket,
    lambda_functions=[
        {
            "lambda_function_arn": lambda_handler.arn,
            "events": ["s3:ObjectCreated:*"],
            "filter_suffix": ".csv",
        }
    ],
    opts=pulumi.ResourceOptions(depends_on=[lambda_permission]),
)


snowflake_resources = setup_snowflake_resources(output_bucket.bucket)

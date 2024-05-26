import json
import pulumi
import pulumi_aws as aws
import pulumi_snowflake as snowflake

ROLE_NAME = "snowflake-storage-integration"

config = pulumi.Config()

def setup_snowflake_resources(s3_bucket_name):
    snowflake_user = aws.iam.User("snowflakeUser")

    s3_policy_document = s3_bucket_name.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetBucketLocation",
                            "s3:GetObject",
                            "s3:GetObjectVersion",
                            "s3:ListBucket",
                        ],
                        "Resource": [f"arn:aws:s3:::{arn}/*", f"arn:aws:s3:::{arn}"],
                    }
                ],
            }
        )
    )

    # Create the IAM policy with the dynamically created policy document
    s3_policy = aws.iam.Policy("s3Policy", policy=s3_policy_document)

    # Attach policy to the user
    user_policy_attachment = aws.iam.UserPolicyAttachment(
        "userPolicyAttachment", user=snowflake_user.name, policy_arn=s3_policy.arn
    )

    # Create access key for the user
    snowflake_user_key = aws.iam.AccessKey("snowflakeUserKey", user=snowflake_user.name)

    # Retrieve configuration using secrets where appropriate
    snowflake_account = config.require("snowflake_account")
    snowflake_username = config.require("snowflake_user")
    snowflake_password = config.require_secret("snowflake_password")

    # Configuring the Snowflake provider with secrets
    snowflake_provider = snowflake.Provider(
        "snowflakeProvider",
        account=snowflake_account,
        user=snowflake_username,
        password=snowflake_password,
        role="ACCOUNTADMIN",
    )

    # Define resources using provider
    warehouse = snowflake.Warehouse(
        "warehouse",
        name="customers_wh",
        warehouse_size="X-SMALL",
        auto_suspend=120,
        auto_resume=True,
        opts=pulumi.ResourceOptions(provider=snowflake_provider),
    )

    database = snowflake.Database(
        "database",
        name="customers_db",
        opts=pulumi.ResourceOptions(provider=snowflake_provider),
    )

    schema = snowflake.Schema(
        "schema",
        name="customers_schema",
        database=database.name,
        opts=pulumi.ResourceOptions(provider=snowflake_provider),
    )

    table = snowflake.Table(
        "table",
        database=database.name,
        schema=schema.name,
        name="customers",
        columns=[
            {"name": "customerid", "type": "NUMBER"},
            {"name": "namestyle", "type": "BOOLEAN"},
            {"name": "title", "type": "STRING"},
            {"name": "firstname", "type": "STRING"},
            {"name": "middlename", "type": "STRING"},
            {"name": "lastname", "type": "STRING"},
            {"name": "suffix", "type": "STRING"},
            {"name": "companyname", "type": "STRING"},
            {"name": "salesperson", "type": "STRING"},
            {"name": "emailaddress", "type": "STRING"},
            {"name": "phone", "type": "STRING"},
            {"name": "passwordhash", "type": "STRING"},
            {"name": "passwordsalt", "type": "STRING"},
            {"name": "rowguid", "type": "STRING"},
            {"name": "modifieddate", "type": "TIMESTAMP"},
        ],
        opts=pulumi.ResourceOptions(provider=snowflake_provider),
    )

    stage = snowflake.Stage(
        "Stage",
        name="stage",
        database=database.name,
        schema=schema.name,
        file_format="TYPE = JSON",
        credentials=pulumi.Output.all(
            snowflake_user_key.id, snowflake_user_key.secret
        ).apply(lambda args: f"AWS_KEY_ID='{args[0]}' AWS_SECRET_KEY='{args[1]}'"),
        url=pulumi.Output.format("s3://{0}", s3_bucket_name),
        opts=pulumi.ResourceOptions(provider=snowflake_provider),
    )

    # Create an Amazon SQS queue
    sqs_queue = aws.sqs.Queue("sqsQueue")

    # Create a policy for the SQS queue to allow SNS to send messages
    sqs_queue_policy_document = sqs_queue.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "SQS:SendMessage",
                        "Resource": arn,
                    }
                ],
            }
        )
    )

    sqs_queue_policy = aws.iam.Policy("SQSPolicy", policy=sqs_queue_policy_document)

    aws.iam.UserPolicyAttachment(
        "SQSPolicyAttachment", user=snowflake_user.name, policy_arn=sqs_queue_policy.arn
    )

# Create a Snowpipe to automatically ingest data from the S3 bucket
    copy_statement = pulumi.Output.format(
        """
    COPY INTO \"{0}\".\"{1}\".\"{2}\" 
    FROM @\"{0}\".\"{1}\".\"{3}\" 
    FILE_FORMAT = (TYPE = 'JSON')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """,
        database.name,
        schema.name,
        table.name,
        stage.name,
    )

    snowpipe = snowflake.Pipe(
        "pipe",
        auto_ingest=True,
        copy_statement=copy_statement,
        database=database.name,
        schema=schema.name,
        opts=pulumi.ResourceOptions(
            provider=snowflake_provider, depends_on=[table, schema, database, warehouse]
        ),
    )

    aws.s3.BucketNotification(
        "SQSbucketNotification",
        bucket=s3_bucket_name,
        queues=[
            {
                "queue_arn": snowpipe.notification_channel,
                "events": ["s3:ObjectCreated:*"],
            }
        ],
    )

    return {
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "table": table,
        "stage": stage,
        "snowpipe": snowpipe,
    }

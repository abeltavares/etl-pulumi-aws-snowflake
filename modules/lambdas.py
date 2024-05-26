import pulumi
import pulumi_aws as aws
import json


def setup_lambda_roles_and_policies(glue_job_name, glue_crawler_name, s3_bucket_arn):
    lambda_role = aws.iam.Role(
        "lambdaRoleForGlueTrigger",
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    # Attach AWSLambdaBasicExecutionRole to the lambda role
    aws.iam.RolePolicyAttachment(
        "lambdaExecutionRoleAttachment",
        role=lambda_role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )

    # Create and attach a policy for Glue job execution and S3 access
    policy_document = pulumi.Output.all(
        glue_job_name, glue_crawler_name, s3_bucket_arn
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["glue:StartJobRun"],
                        "Resource": [f"arn:aws:glue:*:*:job/{args[0]}"],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["glue:StartCrawler", "glue:GetCrawler"],
                        "Resource": [f"arn:aws:glue:*:*:crawler/{args[1]}"],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [
                            f"arn:aws:s3:::{args[2]}/*",
                            f"arn:aws:s3:::{args[2]}",
                        ],
                    },
                ],
            }
        )
    )
    glue_policy = aws.iam.Policy("gluePolicy", policy=policy_document)

    aws.iam.RolePolicyAttachment(
        "lambdaGluePolicyAttachment", role=lambda_role.name, policy_arn=glue_policy.arn
    )

    # Additionally, create and attach a policy for S3 read access specific to the data lake bucket
    s3_read_policy_document = s3_bucket_arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": f"arn:aws:s3:::{arn}/*",
                    }
                ],
            }
        )
    )
    s3_read_policy = aws.iam.Policy(
        "lambdaS3ReadPolicy", policy=s3_read_policy_document
    )

    aws.iam.RolePolicyAttachment(
        "lambdaS3ReadPolicyAttachment",
        role=lambda_role.name,
        policy_arn=s3_read_policy.arn,
    )

    return lambda_role


def create_lambda_function(
    function_name,
    role_arn,
    handler_name,
    s3_bucket_name,
    s3_key,
    glue_crawler_name,
    glue_job_name,
    runtime="python3.8",
):
    lambda_func = aws.lambda_.Function(
        function_name,
        runtime=runtime,
        role=role_arn,
        handler=handler_name,
        s3_bucket=s3_bucket_name,
        s3_key=s3_key,
        environment={
            "variables": {
                "GLUE_CRAWLER_NAME": glue_crawler_name,
                "GLUE_JOB_NAME": glue_job_name,
            }
        },
    )

    return lambda_func


def upload_lambda_code(bucket_name, file_path):
    lambda_code = aws.s3.BucketObject(
        "lambdaCode",
        bucket=bucket_name,
        source=pulumi.FileAsset(file_path),
        key="lambda/",
    )
    return lambda_code

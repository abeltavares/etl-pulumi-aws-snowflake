import json
import pulumi
import pulumi_aws as aws


def setup_crawler(bucket, glue_database, provider=None):
    # Create a role for the AWS Glue Crawler
    aws_glue_crawler_role = aws.iam.Role(
        "AWSGlueCrawlerRole",
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
        opts=pulumi.ResourceOptions(provider=provider) if provider else None,
    )

    # Attach the policy that allows logs:PutLogEvents
    aws.iam.RolePolicy(
        "AWSGlueCrawlerLoggingPolicy",
        role=aws_glue_crawler_role.id,
        policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:PutLogEvents",
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                        ],
                        "Resource": "arn:aws:logs:*:*:*",
                    }
                ],
            }
        ),
    )

    # Attach the policy that allows s3:GetObject, s3:ListBucket, and Glue access
    aws.iam.RolePolicy(
        "AWSGlueCrawlerS3GlueAccessPolicy",
        role=aws_glue_crawler_role.id,
        policy=pulumi.Output.all(bucket, glue_database.name).apply(
            lambda args: json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:ListBucket"],
                            "Resource": [
                                f"arn:aws:s3:::{args[0]}/*",
                                f"arn:aws:s3:::{args[0]}",
                            ],
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "glue:GetDatabase",
                                "glue:GetTables",
                                "glue:GetTable",
                                "glue:StartCrawler",
                                "glue:BatchGetCrawlers",
                                "glue:CreateTable",
                            ],
                            "Resource": [
                                f"arn:aws:glue:*:*:catalog",
                                f"arn:aws:glue:*:*:database/{args[1]}",
                                f"arn:aws:glue:*:*:table/{args[1]}/*",
                            ],
                        },
                    ],
                }
            )
        ),
    )

    # Create the Glue Crawler
    crawler = aws.glue.Crawler(
        "glueCrawler",
        role=aws_glue_crawler_role.arn,
        database_name=glue_database.name,
        s3_targets=[
            aws.glue.CrawlerS3TargetArgs(
                path=bucket.apply(lambda b: f"s3://{b}/"),
            )
        ],
        opts=pulumi.ResourceOptions(provider=provider) if provider else None,
    )
    return crawler


def setup_database(provider=None):
    aws_glue_database_name = pulumi.Config().require("aws_glue_database_name")

    glue_database = aws.glue.CatalogDatabase(
        "glueDatabase",
        name=aws_glue_database_name,
        opts=pulumi.ResourceOptions(provider=provider) if provider else None,
    )
    return glue_database


def upload_glue_code(bucket_name, script_path):
    return aws.s3.BucketObject(
        "GlueJobScript",
        bucket=bucket_name,
        source=pulumi.FileAsset(script_path),
        key="glue/",
    )


def setup_job(
    data_lake_bucket, output_bucket, scripts_bucket, script_path, provider=None
):
    # Create a role for the AWS Glue Job
    glue_job_role = aws.iam.Role(
        "AWSGlueJobRole",
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    # Attach the Glue Service Role policy
    aws.iam.RolePolicyAttachment(
        "GlueJobPolicyAttachment",
        role=glue_job_role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    )

    # Create and attach a policy for S3 access
    scripts_bucket_name = pulumi.Config().require("scripts_bucket_name")

    s3_access_policy_document = pulumi.Output.all(
        data_lake_bucket, output_bucket, scripts_bucket_name
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                        "Resource": [
                            f"arn:aws:s3:::{args[2]}/*",
                            f"arn:aws:s3:::{args[0]}/*",
                            f"arn:aws:s3:::{args[1]}/*",
                        ],
                    }
                ],
            }
        )
    )
    s3_access_policy = aws.iam.Policy(
        "S3AccessPolicy", policy=s3_access_policy_document
    )

    aws.iam.RolePolicyAttachment(
        "S3AccessPolicyAttachment",
        role=glue_job_role.name,
        policy_arn=s3_access_policy.arn,
    )

    # Create and attach a policy for Glue Catalog access
    glue_catalog_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:GetTables", "glue:GetDatabase"],
                    "Resource": ["*"],
                }
            ],
        }
    )

    glue_catalog_policy = aws.iam.Policy(
        "GlueCatalogAccessPolicy", policy=glue_catalog_policy_document
    )

    aws.iam.RolePolicyAttachment(
        "GlueCatalogAccessPolicyAttachment",
        role=glue_job_role.name,
        policy_arn=glue_catalog_policy.arn,
    )

    # Create the Glue Job
    glue_job = aws.glue.Job(
        "MyGlueJob",
        role_arn=glue_job_role.arn,
        command=aws.glue.JobCommandArgs(
            name="glueetl",  # Use 'glueetl' for Python shell jobs
            script_location=pulumi.Output.concat(
                "s3://", scripts_bucket, "/", script_path
            ),
            python_version="3",
        ),
        max_capacity=2.0,  # Specify the capacity for the job. This should be a value between 2.0 and 100.0
        glue_version="4.0",  # Specify the Glue version. This should be '0.9', '1.0', or '2.0'
        opts=pulumi.ResourceOptions(provider=provider) if provider else None,
    )
    return glue_job

import pulumi
import pulumi_aws as aws

config = pulumi.Config()

def setup_s3_buckets():
    data_lake_bucket = aws.s3.Bucket(
        "dataLakeBucket",
        bucket=config.require("s3_bucket_name"),
        versioning=aws.s3.BucketVersioningArgs(enabled=True),
        tags={"Environment": config.require("environment")},
    )

    output_bucket = aws.s3.Bucket(
        "OutputBucket",
        bucket=config.require("output_bucket_name"),
        versioning=aws.s3.BucketVersioningArgs(enabled=True),
        tags={"Environment": config.require("environment")},
    )

    scripts_bucket = aws.s3.Bucket(
        "scriptsBucket",
        bucket=config.require("scripts_bucket_name"),
        versioning=aws.s3.BucketVersioningArgs(enabled=True),
        tags={"Purpose": "Lambda and Glue Scripts"},
    )

    return data_lake_bucket, output_bucket, scripts_bucket

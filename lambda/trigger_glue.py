import boto3
import os
import time
import botocore.exceptions


def handler(event, context):
    glue_client = boto3.client("glue")
    glue_crawler_name = os.getenv("GLUE_CRAWLER_NAME")
    glue_job_name = os.getenv("GLUE_JOB_NAME")

    try:
        # Get the current state of the Glue Crawler
        crawler_status = glue_client.get_crawler(Name=glue_crawler_name)["Crawler"][
            "State"
        ]

        # Start the Glue Crawler only if it is not already running
        if crawler_status != "RUNNING":
            glue_client.start_crawler(Name=glue_crawler_name)
            print(f"Started Glue crawler: {glue_crawler_name}")
        else:
            print(f"Glue crawler {glue_crawler_name} is already running.")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "CrawlerRunningException":
            print(f"Glue crawler {glue_crawler_name} is already running.")
        else:
            raise

        # Wait for the Glue Crawler to finish running
        while True:
            crawler_metadata = glue_client.get_crawler(Name=glue_crawler_name)
            if crawler_metadata["Crawler"]["State"] != "RUNNING":
                break
            time.sleep(
                60
            )  # Wait for 60 seconds before checking the Crawler status again

    # Start the Glue job
    try:
        response = glue_client.start_job_run(JobName=glue_job_name)
        print(f"Glue job started successfully: {response['JobRunId']}")
        return {
            "statusCode": 200,
            "body": f"Glue job {glue_job_name} started successfully with JobRunId {response['JobRunId']}",
        }
    except Exception as e:
        print(f"Error in processing: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error in processing: {str(e)}",
        }

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.analytics import Glue
from diagrams.saas.analytics import Snowflake

with Diagram("ETL Pipeline", show=False):
    s3_data_lake = S3("S3 Data Lake")
    s3_output = S3("S3 Output Bucket")

    with Cluster("Glue ETL"):
        glue_crawler = Glue("Glue Crawler")
        glue_job = Glue("Glue Job")

    lambda_trigger = Lambda("Trigger Glue")
    snowflake = Snowflake("Snowflake")

    s3_data_lake >> Edge(label="trigger") >> lambda_trigger
    lambda_trigger >> Edge(label="start") >> glue_crawler
    glue_crawler >> glue_job
    glue_job >> s3_output
    s3_output >> snowflake

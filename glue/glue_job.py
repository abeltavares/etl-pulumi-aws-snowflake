import sys
import logging
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)

try:
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # DataSource: Read from Glue Catalog
    datasource0 = glueContext.create_dynamic_frame.from_catalog(
        database="metadata_db",
        table_name="data_lake_costumers",
        transformation_ctx="datasource0",
    )

    # Convert to Spark DataFrame to utilize DataFrame operations
    df = datasource0.toDF()

    # Convert CSV format to JSON
    df.write.mode("overwrite").format("json").save("s3://customers-output-bucket/output/")
    job.commit()

except Exception as e:
    logging.error("An error occurred in the Glue job: %s", str(e))
    sys.exit(1)

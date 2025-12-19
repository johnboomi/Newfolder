import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Hardcoded for testing
SOURCE_PATH = "s3://ennodaaccountinnaikku/emp.csv"
TARGET_PATH = "s3://ennodaaccountinnaikku/output/new_folder/"

df = spark.read.option("header", "true").csv(SOURCE_PATH)
df.write.mode("overwrite").option("header", "true").csv(TARGET_PATH)

job.commit()
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

dyf = glueContext.create_dynamic_frame.from_catalog(database='glue-etl-project', table_name='raw_data')

#change dynamicframe to spark df
df = dyf.toDF()

#drop columns we do not need
df = df["id","year_birth","education","marital_status","income","dt_customer"]

#change column names
df_renamed = df.withColumnRenamed("year_birth", "birth_year") \
            .withColumnRenamed("education", "education_level") \
            .withColumnRenamed("dt_customer", "customer")

#change from spark df to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_renamed, glueContext, "glue_etl")

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink( path="<path-to-s3-sink-folder>", connection_type="s3",  updateBehavior="UPDATE_IN_DATABASE",partitionKeys=[],  compression="snappy",  enableUpdateCatalog=True,  transformation_ctx="s3output")

s3output.setCatalogInfo(catalogDatabase="glue-etl-project", catalogTableName="glue-etl-project-table")
s3output.setFormat("glueparquet")
s3output.writeFrame(glue_dynamic_frame_final)
job.commit()

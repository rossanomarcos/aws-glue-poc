#########################################################
# IMPORT LIBS AND SET VARIABLES
#########################################################

# Import python modules
import sys

# Import pyspark modules
from pyspark.context import SparkContext

# Import glue modules
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Parameters
glue_db = "poc-glue-db"
glue_tbl = "poc_s3_storecsvdata"
s3_write_path = "s3://poc-s3-transformedfiles"

#########################################################
# EXTRACT (READ DATA)
#########################################################
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database=glue_db, table_name=glue_tbl)

#########################################################
# TRANSFORMATION (MODIFY DATA)
#########################################################
# convert dynamic frame to data frame to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()

# convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame, glue_context, "dynamic_frame_write")

# apply cleaning
applymapping1   = ApplyMapping.apply(frame = dynamic_frame_read, mappings = [("dispatching_base_num", "string", "dispatching_base_num", "string"), ("pickup_date", "string", "pickup_date", "string"), ("locationid", "string", "locationid", "string")])
resolvechoice2  = ResolveChoice.apply(frame = applymapping1, choice = "make_struct")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2)

#########################################################
# LOAD (WRITE DATA)
#########################################################
datasink4 = glue_context.write_dynamic_frame.from_options(
            frame=dropnullfields3,
            connection_type = "s3",
            connection_options = {"path": s3_write_path},
            format = "parquet")

job.commit()
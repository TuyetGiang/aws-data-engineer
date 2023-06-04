import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1685891763725 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="giang-stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedZone_node1685891763725",
    )
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1685891762002 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="giang-stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1685891762002",
    )
)

# Script generated for node Join
Join_node1685891836371 = Join.apply(
    frame1=AccelerometerTrustedZone_node1685891762002,
    frame2=StepTrainerTrustedZone_node1685891763725,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1685891836371",
)

# Script generated for node Machine Learning Curated Zone
MachineLearningCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1685891836371,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://giang-stedi-human-balance-analytics/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCuratedZone_node3",
)

job.commit()

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

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://giang-stedi-human-balance-analytics/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1685880300872 = glueContext.create_dynamic_frame.from_catalog(
    database="giang-stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1685880300872",
)

# Script generated for node Join
Join_node1685880633228 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=CustomerCuratedZone_node1685880300872,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1685880633228",
)

# Script generated for node Drop Fields
DropFields_node1685880652300 = DropFields.apply(
    frame=Join_node1685880633228,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1685880652300",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685880652300,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://giang-stedi-human-balance-analytics/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()

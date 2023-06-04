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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="giang-stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1685806516269 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [
                "s3://giang-stedi-human-balance-analytics/accelerometer/landing/"
            ],
            "recurse": True,
        },
        transformation_ctx="AccelerometerLandingZone_node1685806516269",
    )
)

# Script generated for node Join Customer
JoinCustomer_node1685806614373 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerLandingZone_node1685806516269,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1685806614373",
)

# Script generated for node Drop Fields
DropFields_node1685806673004 = DropFields.apply(
    frame=JoinCustomer_node1685806614373,
    paths=["user", "x", "y", "z", "timeStamp"],
    transformation_ctx="DropFields_node1685806673004",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685806673004,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://giang-stedi-human-balance-analytics/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node3",
)

job.commit()

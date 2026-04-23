import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read processed data
df = glueContext.create_dynamic_frame.from_catalog(
    database="datalake_db",
    table_name="transformed"
).toDF()

# Select required columns
df = df.select(
    "id",
    "name",
    "department",
    "location",
    "salary",
    "salary_category"
)

# Optional: filter high-value employees
df = df.filter(col("salary") > 40000)

# Write to curated zone
df.write.mode("overwrite").parquet(
    "s3://<s3-curated-bucket-name>/final/"
)

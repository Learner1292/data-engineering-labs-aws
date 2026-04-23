import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, upper, when

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Glue Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="datalake_db",
    table_name="incoming"
).toDF()

# Remove null salary
df = df.filter(col("salary").isNotNull())

# Convert names to uppercase
df = df.withColumn("name", upper(col("name")))

# Add salary category
df = df.withColumn(
    "salary_category",
    when(col("salary") < 45000, "Low")
    .when((col("salary") >= 45000) & (col("salary") <= 65000), "Medium")
    .otherwise("High")
)

# Filter active employees
df = df.filter(col("status") == "active")

# Write to processed zone in Parquet
df.write.mode("overwrite").parquet(
    "s3://<s3-processed-bucket-name>/transformed/"
)

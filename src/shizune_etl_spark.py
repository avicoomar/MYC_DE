"""
To to be run on Spark Runtime
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from itertools import zip_longest
import shizune_crawler
import shizune_transformations

spark = SparkSession.builder.appName("Shizune_etl_spark_app").getOrCreate()

# Read input 
shizune_data = shizune_crawler.get_shizune_data()
schema = StructType([
    StructField('name',
                StringType(), True),
    StructField('meta',
                StringType(), True),
    StructField('links',
                ArrayType(StringType()), True),
    StructField('desc',
                StringType(), True),
    StructField('focus',
               ArrayType(StringType()), True),
    StructField('portfolio_highlights',
                ArrayType(StringType()), True),
])

df = spark.createDataFrame(list(zip_longest(
    shizune_data["investor_names"],
    shizune_data["investor_metas"],
    shizune_data["investor_links"],
    shizune_data["investor_descs"],
    shizune_data["investment_focus_list"],
    shizune_data["portfolio_highlights_list_with_links"],
    fillvalue=None
)), 
schema=schema)

# Run transformation
transformed_dfs = shizune_transformations.transform_dataframes(df)

# Load Data:
transformed_dfs["df"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/shizune_investors")

transformed_dfs["df_desc"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/shizune_investors_desc")

transformed_dfs["df_portfolio_desc"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/shizune_investors_portfolio_desc")
    
spark.stop()

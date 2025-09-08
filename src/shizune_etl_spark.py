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

# TEMP
transformed_dfs["df"].show()
transformed_dfs["df_desc"].show()
transformed_dfs["df_portfolio_desc"].show()

# Load Data:

spark.stop()

"""
To to be run on Spark Runtime
"""
from pyspark.sql import SparkSession
import myc_fse_transformations

spark = SparkSession.builder.appName("MYC_fse_etl_spark_app").getOrCreate()

# Read input 
auth_group = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "auth_group") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

auth_group_permissions = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "auth_group_permissions") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

auth_permission = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "auth_permission") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_company = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_company") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_entrepreneurdetail = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_entrepreneurdetail") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_investordetail = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_investordetail") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_myuser = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_myuser") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_myuser_groups = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_myuser_groups") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()

users_myuser_user_permissions = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://127.0.0.1:3306/MYC_Django") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "users_myuser_user_permissions") \
    .option("user", "gand") \
    .option("password", "gand") \
    .load()


# Run transformation
dataframes = {
	"auth_group": auth_group,
	"auth_group_permissions": auth_group_permissions,
	"auth_permission": auth_permission,
	"users_company": users_company,
	"users_entrepreneurdetail": users_entrepreneurdetail,
	"users_investordetail": users_investordetail,
	"users_myuser": users_myuser,
	"users_myuser_groups": users_myuser_groups,
	"users_myuser_user_permissions": users_myuser_user_permissions
}
transformed_dataframes = myc_fse_transformations.transform(dataframes)


# Load Data:
transformed_dataframes["users_myuser"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_users_myuser")

transformed_dataframes["users_investordetail"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_users_investordetail")

transformed_dataframes["user_ent_comp"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_user_ent_comp")

transformed_dataframes["user_facts"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_user_facts")

transformed_dataframes["auth_group"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_auth_group")

transformed_dataframes["auth_permission"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_auth_permission")

transformed_dataframes["user_permission_facts"].write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://elasticbeanstalk-ap-south-1-576190372469/myc/silver/fse_user_permission_facts")


spark.stop()

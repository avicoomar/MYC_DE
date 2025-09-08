# Setup notes for Visualizaton/Development:
- Create a virtual environment if not created, refer: https://docs.python.org/3/library/venv.html
- Activate the virtual environment
- Install dependencies mentioned in requirements.txt if not installed, run:
  ~~~
  pip install -r requirements.txt
  ~~~
- Create your own "local" folder as per your system configuration in the project's root directory. It should have the following content: 
  - downloaded-spark:  Refer -> https://spark.apache.org/downloads.html 
  - jars folder having the required jars downloaded: mysql-connector, hadoop-aws, aws-sdk-bundle
  - set_env.sh: used for setting SPARK_HOME & PYTHONPATH environment variables
  - set_aws_credentials.sh: used for setting AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY environment variables
- If reading/writing on AWS, then run:
  ~~~
  source local/set_aws_credentials.sh
  ~~~
  
# Visualization/exploration:
~~~
source local/set_env.sh
jupyter notebook --no-browser 
~~~

# Development:
## spark runtime:
1. MYC_FSE pipeline:
~~~
spark-submit \
  --conf spark.master=xyz_cluster \
  --conf spark.jars=./local/jars/mysql-connector-j-9.4.0.jar,./local/jars/hadoop-aws-3.4.1.jar,./local/jars/bundle-2.28.26.jar \
  src/myc_fse_etl_spark.py
~~~
2. MYC_Shizune pipeline:
~~~
spark-submit \
  --conf spark.master=xyz_cluster \
  --conf spark.jars=./local/jars/hadoop-aws-3.4.1.jar,./local/jars/bundle-2.28.26.jar \
  src/shizune_etl_spark.py
~~~
  
## aws glue runtime (tentative):
1. Pull aws's official glue local dev docker image (preferrably into local/ dir)
2. Run job_aws_glue.py in that image like: "docker run -it spark-submit src/etl_aws_glue.py"

# Testing:
nil as of now

# Production(tentative):
## spark runtime:
In an EC2 instance having spark runtime, run: 
~~~
spark-submit src/etl_spark.py --master xyz_cluster
~~~
## aws glue runtime:
Let AWS Glue pick up etl_aws_glue.py (uploaded/deployed to s3) and run it.

import pyspark
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
from dotenv import dotenv_values
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from pyspark.sql.functions import explode,from_json,col
from pyspark.sql.types import StringType,IntegerType,StructType,StructField
current_date=str(datetime.now())
env_var=dotenv_values('.env')
access_key=env_var['access_key']
secret_key=env_var['secret_key']

def create_sparksession():
    spark = SparkSession.builder.appName('Streaming Pipeline')\
                .config('spark.jars.packages',env_var['spark_jar'])\
                 .getOrCreate()
    # Enable hadoop s3a settings
    spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    return spark

def schema():
    columns = StructType([StructField('first_name',
    StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('address', StringType(), False),
    StructField('email', StringType(), False),
    StructField('credit_no', IntegerType(), False),
    StructField('company', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('price',IntegerType(), False)])
    return columns

def read_stream():
    df=create_sparksession().readStream.format("kafka").option("kafka.bootstrap.servers", env_var['bootstrap_server']) \
    .option("subscribe", env_var['topic_name']) \
    .option('includeHeaders','true')\
    .load()
    df1=df.selectExpr("CAST(value AS STRING)",'headers')
    df2=df1.select(from_json('value',schema=schema()).alias('temp')).select('temp.*')
    return df2

def write_stream():
    read_stream().writeStream.format('csv').option('path',env_var['bucket_uri'])\
    .option('checkpointLocation',env_var['checkpoint_uri'])\
    .start().awaitTermination()
    print('Streaming to S3')


    
print(write_stream())

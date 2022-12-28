# batch-streaming-data-pipeline
![pipeline_architecture](https://user-images.githubusercontent.com/57522480/209829110-0edfb68f-7e39-4f1b-8c34-e97b63246c0f.png)


Building both a batch and streaming data pipeline using Kafka, docker, Spark and AWS.

## PROJECT DESCRIPTION
In this project, we created continous  streams of data  using the python's Faker library. These streams were then pushed to a Kafka topic deployed on a remote docker container. When these data streams come into the Kafka topic, the streams are then transformed using Apache Spark's structured streaming. This transformed data is converted to a dataframe and pushed into Amazon S3 buckets as a part csv file. 
These csv files in the s3 bucket are then processed by Aws Glue on a daily basis (batch) and the processed data moved into another s3 bucket.

## PROCEDURES
Create a kafka topic from the Docker container. You can start the docker container by running 

<b>docker-compose up -d</b>

To see if the container is running, you can run 
<b> docker ps</b>
This lists out all the running containers.

From here, you will be able to get the container id, container name, ports and so on.

You can create the kafka topic by navigating to the docker container

docker exec -it {container_id} bin/bash

This command opens up a terminal similar to your regular terminal. This terminal consists of all the directories. You can change directory into opt/bitnami/kafka/bin and run the command

kafka-topics.sh \
 --bootstrap-server localhost:9092 \
 --topic stream_batch_topic \
 --partitions 3 \
 --replication-factor 1 \
 --create

Here, we created a topic with name <b>stream_batch_topic


The code for producing messages to this topic is in streaming_pipeline/producer.py

<img width="1226" alt="Screen Shot 2022-12-27 at 8 29 13 PM" src="https://user-images.githubusercontent.com/57522480/209832209-17a9f1dc-395c-4c17-b510-d9580ad97078.png">

The spark_consumer.py was used to push the live streams into S3 bucket.
You can modify the .env file and put in your bucket name and details there. 

The output stream in the s3 bucket is shown in the image below
<img width="1123" alt="Screen Shot 2022-12-28 at 2 18 10 PM" src="https://user-images.githubusercontent.com/57522480/209832457-360647ca-1b14-41b3-8029-936f79afdddd.png">

A lambda trigger was then created such that any file dropped into s3 , on failure or success, triggers an AWS SNS topic.
<img width="1440" alt="lambda_events" src="https://user-images.githubusercontent.com/57522480/209832593-b903c588-f044-4471-a949-d04fadd8193a.png">

A glue job was also created to process the streamed s3 data daily in batches and upload it to another s3 bucket.
<img width="957" alt="glue_batch_layer" src="https://user-images.githubusercontent.com/57522480/209832832-a7055c76-a7cf-41df-a4b0-5d08b54afec0.png">



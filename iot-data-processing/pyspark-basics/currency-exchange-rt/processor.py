""" Final exercise. Data Processing in IoT.
@author Roberto Hernandez
"""

import json
from datetime import datetime

from influxdb_client import InfluxDBClient, Point  # type: ignore
from influxdb_client.client.write_api import SYNCHRONOUS  # type: ignore
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import lit  # type: ignore
from pyspark.streaming import StreamingContext  # type: ignore
from pyspark.streaming.kafka import KafkaUtils  # type: ignore


def saveToInfluxDB(rdd):
    """Writes processed data to InfluxDB and Cassandra."""
    
    if not rdd.isEmpty():
        
        data = rdd.collect()
        df = spark.createDataFrame(rdd)
        
        for i in data:
    
            omg_eur = float(i.get("omg_eur"))
            omg_usd = float(i.get("omg_usd"))
            
            eur_usd = omg_usd / omg_eur
            usd_eur = omg_eur / omg_usd
            
            timeValue = datetime.strptime(i.get("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
            print(f"Writing omg_eur {omg_eur}, omg_usd {omg_usd}, "
                  f"eur_usd {eur_usd}, usd_eur {usd_eur}, in {timeValue} to InfluxDB")

            point = Point("cambios-divisas").field("omgtoeur", omg_eur).field("omgtousd", omg_usd).field("eurtousd", eur_usd).field("usdtoeur", usd_eur).time(time=timeValue)
            
            influxClient.write_api(write_options=SYNCHRONOUS).write(
                bucket=bucket, record=point)

            # Write in Cassandra table
            df = df.withColumn("eur_usd", lit(eur_usd)).withColumn("usd_eur", lit(usd_eur))

            (df.write
                .format("org.apache.spark.sql.cassandra")
                .options(keyspace="test", table="divisas")
                .mode("append")
                .save())


# Local SparkContext and StreamingContext
spark = (SparkSession
         .builder
         .master("local[*]")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate())

sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)


# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "100449779's Bucket"

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: get JSON values
out = (stream.map(lambda x: json.loads(x[1])))

# 3. Output data: store results in InfluxDb and Cassandra
out.pprint()
out.foreachRDD(saveToInfluxDB)

ssc.start()
ssc.awaitTermination()

""" Exercise 2B. Data Processing in IoT.
@author Roberto Hernandez
"""

from pyspark import SparkContext, SparkConf  # type: ignore
from pyspark.streaming import StreamingContext  # type: ignore
from pyspark.streaming.kafka import KafkaUtils  # type: ignore

# Local SparkContext and StreamingContext (batch interval of 5 seconds)
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"))

sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})


# 2. Data processing: filter only last digit and show it if it is prime
def isPrime(num):
    num = int(num)
    if num > 1:
        for i in range(2, num):
            if (num % i) == 0:
                return False
        else:
            return True
    else:
        return False


count = (stream.map(lambda x: x[1][-1])
         .filter(lambda num: isPrime(num))
         )

# 3. Output data: show result in the console
count.pprint()

ssc.start()
ssc.awaitTermination()

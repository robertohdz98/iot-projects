""" Exercise 1A. Data Processing in IoT.
@author Roberto Hernandez
"""

from pyspark import SparkContext  # type: ignore
from pyspark.streaming import StreamingContext  # type: ignore

# Local SparkContext
sc = SparkContext(master="local[*]", appName="ejercicio1")
sc.setLogLevel("ERROR")

# StreamingContext with a batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# 1. Input data: create a DStream that receives data from a socket
stream = ssc.socketTextStream("localhost", 9999)

# 2. Data processing: delete alphanumeric words
palabras = stream.flatMap(lambda line: line.split(" "))
numeros = palabras.filter(lambda c: c.isdigit())

# 3. Output data: show result in the console
# Print only numbers of each RDD generated in this DStream
numeros.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

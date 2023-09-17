""" Final exercise. Data Processing in IoT.
@author Roberto Hernandez
"""

import json
import time
from datetime import datetime

import cryptocompare  # type: ignore
from kafka import KafkaProducer  # type: ignore

APIKEY = "8ad0ac0ce985feb0aec6707f807cbcaae9fbeccd5d29e9467e3d73765785ed0d"

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0
cryptocompare.cryptocompare._set_api_key_parameter(APIKEY)


while True:
    
    # Change from OMG Network to Euros and USD
    changes = cryptocompare.get_price('OMG', ['EUR', 'USD'])
    omg_eur = changes['OMG']['EUR']
    omg_usd = changes['OMG']['USD']

    msg = {"timestamp": str(datetime.utcnow()),
           "omg_eur": omg_eur,
           "omg_usd": omg_usd}
    
    print("Sending bitcoin changes to Kafka", msg)
    
    producer.send("test-topic", json.dumps(msg).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))

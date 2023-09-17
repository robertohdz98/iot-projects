"""
Informatics Systems in IoT Course:
"POC of IoT system for monitoring drug stores in pharmacies"
@author: Roberto Hernandez Ruiz
"""

import requests  # type: ignore
from flask import Flask, request

app = Flask(__name__)

numPresences = 0
url = "http://192.168.1.150"


@app.route('/sensor_values', methods=['GET', 'POST'])
def read_sensors():
    global numPresences
    
    if request.method == 'POST':
        content = request.get_json()
        content['temperature'] = round(content['temperature'], 2)
        content['pressure'] = round(content['pressure'], 2)
        content['humidity'] = round(content['humidity'], 2)
        
        # Actuadores
        if content['temperature'] > 25:
            requests.get(url + '/T')
            print('TEMPERATURA SUPERIOR A 25ºC')
        
        if content['temperature'] < 20:
            requests.get(url + '/L')
            print('TEMPERATURA INFERIOR A 20ºC')
            
        if content['presencia'] == "True":
            numPresences = numPresences + 1
            requests.get(url + '/P')
            print('SE HA DETECTADO MOVIMIENTO. PRESENCIA ' + str(numPresences))
            
        if content['humidity'] > 45:
            requests.get(url + '/H')
            print('HUMEDAD ELEVADA')

        url_string = 'http://localhost:8086/write?db=proyecto'
        datapoint = 'datos,localizacion=Farmacia temperature=' + str(content['temperature']) + ',pressure=' + str(content['pressure']) + ',humidity=' + str(content['humidity']) + ',Detecciones=' + str(numPresences)
        print(datapoint)
        requests.post(url_string, datapoint)
        
        return "Received"


app.run(host="0.0.0.0", port="5000")

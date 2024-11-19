import requests
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import json
import time

'''
Sensores que funcionan:

10.1.1.7
10.1.1.1
10.1.1.11
10.1.1.12
01749
10118 Obtiene muy poca info
01548
10115 Obtiene muy poca info
01870
01575
01737
01550
01534
01535
01509
01529
01528
'''

# Configuración del broker y el topic
broker = "broker.emqx.io"  # Cambia esto si usas otro broker
port = 1883  # Puerto estándar para MQTT
topic = "redes-proyecto-iot/decib"

sensors = ['01528', '01529', '01509']

client = mqtt.Client(protocol=mqtt.MQTTv311)

# Configuración de acceso a la API
username = 'dublincityapi'
password = 'Xpa5vAQ9ki'

# Configuración del intervalo de tiempo (por ejemplo, última hora)
end_time = int(datetime.now().timestamp())  # Tiempo actual en formato UNIX
start_time = int((datetime.now() - timedelta(minutes=30)).timestamp())  # Hace 1 hora


for sensor in sensors:

    # Parámetros de la solicitud
    data_params = {
        'username': username,
        'password': password,
        'monitor': sensor,
        'start': start_time,
        'end': end_time
    }

    sensor_info_params = {
        'username': username,
        'password': password,
        'monitor': sensor,        
    }

    # Realizar la solicitud POST
    data_response = requests.post('https://data.smartdublin.ie/sonitus-api/api/data', params=data_params)

    #print(data_response.json())

    # Manejar la respuesta
    if data_response.status_code == 200:
        data = data_response.json()
        last_value = data[-1]
        #print("Lecturas de decibeles (dB):")

        #print(f"Fecha y hora: {last_value['datetime']}, dB: {last_value['laeq']}")

        decibels = last_value['laeq']
        date_time = last_value['datetime'].split(" ")[-1]

    else:
        print(f"Error {data_response.status_code}: {data_response.text}")

    # Realizar la solicitud POST
    sensor_response = requests.post(f'https://data.smartdublin.ie/sonitus-api/api/monitor/{sensor}', params=sensor_info_params)

    # Manejar la respuesta
    if sensor_response.status_code == 200:
        data = sensor_response.json()
        name = data['location']
        lat = float(data['latitude'])
        lon = float(data['longitude'])
    else:
        print(f"Error {sensor_response.status_code}: {sensor_response.text}")

    # Datos en formato JSON
    datos = {
        "dB": decibels,
        "datetime": date_time,
        "location": name,
        "lat": lat,
        "lon": lon
    }

    mensaje = json.dumps(datos)

    print(mensaje)

    client.connect(broker, port)
    
    client.loop_start()

    result = client.publish(topic, mensaje)

    status = result.rc
    if status == mqtt.MQTT_ERR_SUCCESS:
        print(f"Mensaje '{mensaje}' enviado al topic '{topic}'")
    else:
        print(f"Error al enviar mensaje al topic '{topic}'")

    # Esperar un momento antes de detener el bucle
    time.sleep(1)
    client.loop_stop()
    client.disconnect()    
    
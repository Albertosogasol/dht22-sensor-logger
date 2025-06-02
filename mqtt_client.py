import paho.mqtt.client as mqtt
import json
import logging
import os
import config_loader  # Asumo que tienes este módulo para cargar config.json

# Obtener el path absoluto del config.json (asumiendo que está en el mismo directorio que mqtt_client.py)
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.json')

# Cargar configuración
try:
    config = config_loader.load_config(config_path)
    MQTT_BROKER = config.get('mqtt', {}).get('broker', 'localhost')
    MQTT_PORT = int(config.get('mqtt', {}).get('port', 1883))
    MQTT_TOPIC = config.get('mqtt', {}).get('topic', 'casa/sensores/temperatura')
except Exception as e:
    logging.error(f"Error al cargar configuración MQTT: {e}")
    MQTT_BROKER = 'localhost'
    MQTT_PORT = 1883
    MQTT_TOPIC = 'casa/sensores/temperatura'


def publish_mqtt(data):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        # Topic original (mantenerlo por compatibilidad)
        payload = json.dumps(data)
        client.publish(MQTT_TOPIC, payload)

        # Nuevos topics separados por ciudad/sensor/tipo
        base_topic = f"casa/{data['city'].lower()}/{data['sensor_type'].lower()}"
        client.publish(f"{base_topic}/temperatura", str(data["temperature"]))
        client.publish(f"{base_topic}/humedad", str(data["humidity"]))
        print(f"Mensaje publicado con exito")

        client.disconnect()
        logging.info(f"Publicado MQTT a múltiples topics: {payload}")
    except Exception as e:
        logging.error(f"Error al publicar en MQTT: {e}")

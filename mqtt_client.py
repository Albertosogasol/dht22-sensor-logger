import paho.mqtt.client as mqtt
import json
import logging
import os
import config_loader  # Módulo propio para cargar config.json

# Obtener el path absoluto del config.json
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.json')

# Configurar logging básico
logging.basicConfig(level=logging.INFO)

# Cargar configuración MQTT
try:
    config = config_loader.load_config(config_path)
    MQTT_BROKER = config.get('mqtt', {}).get('broker', 'localhost')
    MQTT_PORT = int(config.get('mqtt', {}).get('port', 1883))
    MQTT_TOPIC = config.get('mqtt', {}).get('topic', 'casa/sensores/dht22')
except Exception as e:
    logging.error(f"Error al cargar configuración MQTT: {e}")
    MQTT_BROKER = 'localhost'
    MQTT_PORT = 1883
    MQTT_TOPIC = 'casa/sensores/dht22'


def publish_mqtt(data):
    try:
        client = mqtt.Client()
        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        payload = json.dumps(data)

        # 1. Publicar al topic específico según tipo de sensor
        sensor_type = data.get("sensor_type", "desconocido").lower()
        topic_by_type = f"{MQTT_TOPIC}/{sensor_type}"  # Ej: casa/sensores/dht22/interior
        client.publish(topic_by_type, payload)

        # 2. Publicar datos individuales a topics por ciudad/sensor
        city = data.get("city", "desconocida").lower()
        base_topic = f"casa/{city}/{sensor_type}"
        client.publish(f"{base_topic}/temperatura", str(data.get("temperature", "")))
        client.publish(f"{base_topic}/humedad", str(data.get("humidity", "")))

        print(f"Mensaje publicado en {topic_by_type}")
        logging.info(f"Publicado MQTT a múltiples topics: {payload}")

        client.disconnect()

    except Exception as e:
        logging.error(f"Error al publicar en MQTT: {e}")

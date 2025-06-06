import time
import Adafruit_DHT
import sqlite3
import logging
import json
import os
import mqtt_client
from datetime import datetime
import config_loader

log_path = '/log/sensor_log.log'
tmp_log_path = '/tmp/sensor_log.log'

# Obtener el directorio del script actual
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.json')

# Configuración de logging
logging.basicConfig(level=logging.INFO, filename=tmp_log_path, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Variables globales
sensor_pins = {}
location = {}
database = {}
outside_pin = None
inside_pin = None
latitude = None
longitude = None
city = None
location_name = None
country = None
postal_code = None
street = None
db_path = None

def load_configuration():
    """Cargar la configuración del archivo JSON"""
    global sensor_pins, location, database, outside_pin, inside_pin, latitude, longitude
    global city, location_name, country, postal_code, street, db_path

    try:
        config = config_loader.load_config(config_path)
    except Exception as e:
        logging.error(f"No se pudo cargar la configuración: {e}")
        exit(1)

    if config:
        sensor_pins = config["sensor_pins"]
        location = config["location"]
        database = config["database"]
        
        outside_pin = int(sensor_pins["outside"])
        inside_pin = int(sensor_pins["inside"])
        latitude = location["latitude"]
        longitude = location["longitude"]
        city = location["city"]
        location_name = location["location_name"]
        country = location["country"]
        postal_code = location["postal_code"]
        street = location["street"]
        db_path = database["path"]
    else:
        logging.error("No se pudo cargar la configuración.")
        exit(1)

def create_db_connection():
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        return None

def create_table(conn):
    """Crear la tabla si no existe"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS temp_hum_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        sensor_location TEXT,
        temperature REAL,
        humidity REAL,
        sensor_type TEXT,
        latitude REAL,
        longitude REAL,
        city TEXT,
        country TEXT,
        postal_code TEXT,
        street TEXT
    )
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error al crear la tabla: {e}")

def read_sensor(pin):
    for _ in range(1):  # Se puede cambiar a más intentos si se desea
        humidity, temperature = Adafruit_DHT.read_retry(Adafruit_DHT.DHT22, pin)
        if humidity is not None and temperature is not None:
            return round(temperature, 2), round(humidity, 2)
        else:
            logging.warning(f"Fallo en la lectura del sensor en el pin {pin}. Reintentando...")
            time.sleep(5)
    logging.error(f"Fallo en la lectura del sensor en el pin {pin} después de varios intentos.")
    return None, None

def get_valid_measurement(pin, sensor_location):
    for attempt in range(1):  # Se puede aumentar a 5 si quieres más robustez
        temperature, humidity = read_sensor(pin)
        if humidity is not None and 0 <= humidity <= 100:
            return temperature, humidity
        if attempt < 4:
            logging.warning(f"Lectura de humedad fuera de rango. Reintentando... (Intento {attempt + 1}/1)")
            time.sleep(5)
    logging.error(f"No se pudo obtener una medición válida de {sensor_location}.")
    return None, None

def publish_measurement(sensor_type, temperature, humidity, timestamp):
    """Publicar los datos por MQTT"""
    mqtt_client.publish_mqtt({
        "location": location_name,
        "temperature": temperature,
        "humidity": humidity,
        "sensor_type": sensor_type,
        "timestamp": timestamp,
        "latitude": latitude,
        "longitude": longitude,
        "city": city,
        "country": country,
        "postal_code": postal_code,
        "street": street
    })

def insert_data(conn, temperature, humidity, sensor_type, timestamp):
    """Insertar los datos y publicar por MQTT"""
    insert_query = """
    INSERT INTO temp_hum_data (sensor_location, temperature, humidity, sensor_type, 
                                 latitude, longitude, city, country, postal_code, street)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(insert_query, (
            location_name, temperature, humidity, sensor_type,
            latitude, longitude, city, country, postal_code, street
        ))
        conn.commit()
        publish_measurement(sensor_type, temperature, humidity, timestamp)
    except sqlite3.Error as e:
        logging.error(f"Error al insertar datos en la base de datos: {e}")

def main():
    load_configuration()
    now = datetime.now()
    current_minute = now.minute
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    save_to_db = (current_minute % 5 == 0)

    conn = create_db_connection() if save_to_db else None
    if conn:
        create_table(conn)

    for pin, sensor_type in [(outside_pin, "exterior"), (inside_pin, "interior")]:
        temperature, humidity = get_valid_measurement(pin, sensor_type)
        if temperature is not None and humidity is not None:
            if save_to_db and conn:
                insert_data(conn, temperature, humidity, sensor_type, timestamp)
            else:
                publish_measurement(sensor_type, temperature, humidity, timestamp)

    if conn:
        conn.close()

if __name__ == "__main__":
    main()
    print("Ejecución de main en DEV")

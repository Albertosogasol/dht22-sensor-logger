import time
import Adafruit_DHT
import sqlite3
import os
import logging
import json

import config_loader

# Configuración de logging
logging.basicConfig(level=logging.INFO, filename='log/sensor_log.log', 
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

# Cargar la configuración globalmente
def load_configuration():
    """Cargar la configuración del archivo JSON"""
    global sensor_pins, location, database, outside_pin, inside_pin, latitude, longitude
    global city, location_name, country, postal_code, street, db_path

    config = config_loader.load_config()

    if config:
        sensor_pins = config["sensor_pins"]
        location = config["location"]
        database = config["database"]
        
        outside_pin = int(sensor_pins["outside"])  # Convertir a entero
        inside_pin = int(sensor_pins["inside"])    # Convertir a entero
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

# Conexion con la base de datos
def create_db_connection():
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        return None

# Función para crear la tabla si no existe
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

# Lectura de los datos del sensor
def read_sensor(pin):
    # Intentar leer el sensor varias veces
    for _ in range(5):  # Intentar hasta 5 veces
        humidity, temperature = Adafruit_DHT.read_retry(Adafruit_DHT.DHT22, pin)
        if humidity is not None and temperature is not None:
            return temperature, humidity
        else:
            logging.warning(f"Fallo en la lectura del sensor en el pin {pin}. Reintentando...")
        time.sleep(1)  # Esperar 1 segundo antes de reintentar
    logging.error(f"Fallo en la lectura del sensor en el pin {pin} después de varios intentos.")
    return None, None


# Insertar datos en DB
def insert_data(conn, location_name, temperature, humidity, sensor_type):
    """Insertar los datos de temperatura y humedad en la base de datos"""
    insert_query = """
    INSERT INTO temp_hum_data (sensor_location, temperature, humidity, sensor_type, 
                                 latitude, longitude, city, country, postal_code, street)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(insert_query, (location_name, temperature, humidity, sensor_type,  
                                      latitude, longitude, city, country, postal_code, street))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error al insertar datos en la base de datos: {e}")

# Funcion principal
def main():
    load_configuration()

    # Establecer conexión con la base de datos
    conn = create_db_connection()
    if conn:
        create_table(conn)  # Asegurarse de que la tabla existe

        # Leer los datos del sensor exterior y guardarlos en la base de datos
        temperature, humidity = read_sensor(outside_pin)
        if temperature and humidity:
            sensor_location = "exterior"
            insert_data(conn, location_name, temperature, humidity, sensor_location)
    
        # Leer los datos del sensor interior y guardarlos en la base de datos
        temperature, humidity = read_sensor(inside_pin)
        if temperature and humidity:
            sensor_location = "interior"
            insert_data(conn, location_name, temperature, humidity, sensor_location)

if __name__ == "__main__":
    main()

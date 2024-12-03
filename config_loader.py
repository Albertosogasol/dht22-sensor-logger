import json
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, filename='/tmp/sensor_log.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(file_path):
    """Carga la configuración desde un archivo JSON"""
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except Exception as e:
        #print(f"Error al cargar el archivo de configuración: {e}")
        logging.error(f"Error al cargar la configuracion desde config_loader: {e}")
        raise  # Lanza la excepción para ser manejada fuera de esta función

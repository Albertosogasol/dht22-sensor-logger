import json

def load_config(file_path='config.json'):
    # Load JSON configuration

    try:
        with open(file_path,'r') as config_file:
            config = json.load(config_file)
        return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuracion.")
        return None
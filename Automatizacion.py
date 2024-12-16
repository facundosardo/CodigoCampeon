import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
import time
import zipfile  # Para manejar archivos ZIP

# Configuración del archivo de autenticación de Kaggle
kaggle_json_path = r"C:\Users\campeoncodigo\Desktop\ProyectoFinal"
os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(kaggle_json_path)

# Inicializar la API de Kaggle
api = KaggleApi()
api.authenticate()

# Dataset que queremos descargar
dataset = "wyattowalsh/basketball"
dataset_filename = "nba.sqlite"

# Directorio de salida
output_path = r"C:\Users\campeoncodigo\Desktop\ProyectoFinal"
os.makedirs(output_path, exist_ok=True)

# Archivo de registro para la última descarga
log_file_path = os.path.join(output_path, "last_download.log")

# Número de intentos de reintento
max_retries = 3
retry_delay = 5  # segundos

# Obtener la fecha de la última descarga
def get_last_download_date(log_file):
    if os.path.exists(log_file):
        with open(log_file, 'r') as log_file:
            return datetime.fromisoformat(log_file.read().strip())
    return None

# Guardar la fecha de la última descarga
def save_download_date(log_file, date):
    if date is not None:
        with open(log_file, 'w') as log_file:
            log_file.write(date.isoformat())

# Obtener la fecha de modificación del archivo `nba.sqlite`
def get_file_modification_date(file_path):
    if os.path.exists(file_path):
        return datetime.fromtimestamp(os.path.getmtime(file_path))
    return None

# Descargar un archivo con reintentos en caso de fallo
def download_file_with_retries(api, dataset, file_name, path, max_retries, retry_delay):
    for attempt in range(max_retries):
        try:
            print(f"Intentando descargar {file_name}, intento {attempt + 1} de {max_retries}...")
            api.dataset_download_file(dataset, file_name, path=path, force=True)
            return
        except Exception as e:
            print(f"Error en la descarga: {e}")
            if attempt < max_retries - 1:
                print(f"Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                raise e

# Descomprimir el archivo descargado
def extract_zip(file_path, extract_to):
    if zipfile.is_zipfile(file_path):
        print(f"Descomprimiendo {file_path} en {extract_to}...")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print("Descompresión completada.")
        # Elimina el archivo ZIP después de extraerlo
        os.remove(file_path)
    else:
        print(f"El archivo {file_path} no es un ZIP válido.")

# Descargar el archivo `nba.sqlite` si no hay registro previo o si hay una nueva versión
def download_sqlite_if_needed(api, dataset, output_path, sqlite_filename, log_file_path):
    last_download_date = get_last_download_date(log_file_path)
    sqlite_file_path = os.path.join(output_path, sqlite_filename)
    file_mod_date = get_file_modification_date(sqlite_file_path)
    
    print("Descargando dataset desde Kaggle para verificación...")
    temp_output_path = os.path.join(output_path, "temp")
    os.makedirs(temp_output_path, exist_ok=True)
    download_file_with_retries(api, dataset, sqlite_filename, temp_output_path, max_retries, retry_delay)
    
    temp_sqlite_file_path = os.path.join(temp_output_path, sqlite_filename + ".zip")
    latest_mod_date = get_file_modification_date(temp_sqlite_file_path)
    
    # Comparar las fechas de modificación
    if last_download_date is None or (latest_mod_date is not None and latest_mod_date > last_download_date):
        print("Nueva versión del archivo `nba.sqlite` encontrada. Descargando y descomprimiendo...")
        download_file_with_retries(api, dataset, sqlite_filename, output_path, max_retries, retry_delay)
        extract_zip(temp_sqlite_file_path, output_path)  # Descomprimir el archivo ZIP
        save_download_date(log_file_path, latest_mod_date)
        print("Descarga completada y archivo actualizado.")
    else:
        print("No hay una nueva versión del archivo `nba.sqlite` disponible.")
    
    # Limpiar y eliminar el directorio temporal
    if os.path.exists(temp_output_path):
        shutil.rmtree(temp_output_path)

# Descargar `nba.sqlite` si es necesario
download_sqlite_if_needed(api, dataset, output_path, dataset_filename, log_file_path)

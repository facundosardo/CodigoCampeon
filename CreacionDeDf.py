import sqlite3
import pandas as pd
import os
from datetime import datetime
from joblib import Parallel, delayed
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------------------------------------------------
# FUNCION: Auditar y modificar tipos de datos
# ------------------------------------------------------------
def auditar_y_modificar_tipos(dfs):
    logging.info("Iniciando auditoría y modificación de tipos de datos...")
    for tabla, df in dfs.items():
        for columna in df.columns:
            tipo_actual = df[columna].dtype
            if tipo_actual == 'datetime64[ns]':
                logging.info(f"Columna '{columna}' en '{tabla}' ya es tipo datetime.")
            elif tipo_actual == 'object':
                if df[columna].str.isnumeric().all():
                    df[columna] = pd.to_numeric(df[columna], errors='coerce')
                    logging.info(f"Columna '{columna}' en '{tabla}' convertida a numérico.")
                else:
                    df[columna] = df[columna].astype(str)
                    logging.info(f"Columna '{columna}' en '{tabla}' convertida a string.")
            elif tipo_actual == 'float64':
                df[columna] = df[columna].astype('float32')
                logging.info(f"Columna '{columna}' en '{tabla}' convertida a float32.")
            elif tipo_actual == 'int64':
                df[columna] = df[columna].astype('Int64')  # Soporta NaNs
                logging.info(f"Columna '{columna}' en '{tabla}' convertida a Int64.")
            elif tipo_actual == 'bool':
                df[columna] = df[columna].astype(int)
                logging.info(f"Columna '{columna}' en '{tabla}' convertida a tipo BIT.")
    logging.info("Auditoría completada.")
    return dfs

# ------------------------------------------------------------
# FUNCION: Validar y cargar tablas desde SQLite
# ------------------------------------------------------------
def cargar_tablas(ruta_sqlite, tablas):
    logging.info(f"Conectando a la base de datos SQLite en '{ruta_sqlite}'...")
    if not os.path.exists(ruta_sqlite):
        logging.error("El archivo SQLite no existe.")
        return {}
    
    conn = sqlite3.connect(ruta_sqlite)
    dfs = {}
    try:
        for tabla in tablas:
            try:
                dfs[tabla] = pd.read_sql_query(f"SELECT * FROM {tabla}", conn)
                logging.info(f"Tabla '{tabla}' cargada exitosamente con {len(dfs[tabla])} registros.")
            except Exception as e:
                logging.warning(f"No se pudo cargar la tabla '{tabla}': {e}")
    finally:
        conn.close()
    return dfs

# ------------------------------------------------------------
# FUNCION: Convertir columnas a tipo datetime
# ------------------------------------------------------------
def convertir_a_fecha(dfs):
    logging.info("Iniciando conversión de columnas a tipo datetime...")
    for tabla, df in dfs.items():
        for columna in df.columns:
            if 'date' in columna.lower() or 'time' in columna.lower():
                try:
                    df[columna] = pd.to_datetime(df[columna], errors='coerce')
                    logging.info(f"Columna '{columna}' en '{tabla}' convertida a datetime.")
                except Exception as e:
                    logging.warning(f"No se pudo convertir '{columna}' en '{tabla}': {e}")
    return dfs

# ------------------------------------------------------------
# FUNCION: Filtrar fechas entre 2000 y hoy
# ------------------------------------------------------------
def filtrar_por_fecha(dfs, fecha_inicio='2000-01-01'):
    logging.info("Iniciando filtrado por fechas...")
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_actual = pd.to_datetime('today')
    
    for tabla, df in dfs.items():
        for columna in df.columns:
            if 'date' in columna.lower():
                try:
                    df[columna] = pd.to_datetime(df[columna], errors='coerce')
                    dfs[tabla] = df[(df[columna] >= fecha_inicio) & (df[columna] <= fecha_actual)]
                    logging.info(f"Tabla '{tabla}' filtrada por fechas en la columna '{columna}'.")
                except Exception as e:
                    logging.warning(f"No se pudo filtrar '{columna}' en '{tabla}': {e}")
    return dfs

# ------------------------------------------------------------
# FUNCION: Limpiar tablas con valores nulos
# ------------------------------------------------------------
def limpiar_tablas_nulos(dfs, umbral=0.88):
    logging.info("Iniciando limpieza de tablas con valores nulos...")
    informe = []
    for tabla, df in list(dfs.items()):
        null_ratio = df.isnull().mean().mean()
        if null_ratio > umbral:
            informe.append({"tabla": tabla, "proporcion_nulos": null_ratio})
            del dfs[tabla]
            logging.warning(f"Tabla '{tabla}' eliminada (proporción nulos: {null_ratio:.2f}).")
    return dfs, informe

# ------------------------------------------------------------
# FUNCION: Guardar DataFrames en archivos CSV (paralelo)
# ------------------------------------------------------------
def guardar_dataframes_csv(dfs):
    logging.info("Iniciando guardado de tablas en archivos CSV...")
    def guardar_csv(tabla, df):
        try:
            file_path = f"{tabla}.csv"
            df.to_csv(file_path, index=False)
            logging.info(f"Tabla '{tabla}' guardada en '{file_path}'.")
        except Exception as e:
            logging.error(f"No se pudo guardar '{tabla}': {e}")
    
    Parallel(n_jobs=-1)(delayed(guardar_csv)(tabla, df) for tabla, df in dfs.items())

# ------------------------------------------------------------
# EJECUCIÓN FINAL
# ------------------------------------------------------------
if __name__ == "__main__":
    ruta_sqlite = 'C:/Users/campeoncodigo/Desktop/ProyectoFinal/nba.sqlite'
    tablas = ["game", "game_info", "game_summary", "line_score", "other_stats", "team_details", "team_history", "team"]

    # Cargar tablas
    dfs = cargar_tablas(ruta_sqlite, tablas)
    
    # Procesos
    dfs = auditar_y_modificar_tipos(dfs)
    dfs = convertir_a_fecha(dfs)
    dfs = filtrar_por_fecha(dfs)
    dfs, informe = limpiar_tablas_nulos(dfs)

    # Exportar a CSV
    guardar_dataframes_csv(dfs)
    logging.info("Proceso finalizado.")

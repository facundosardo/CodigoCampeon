import pandas as pd
from sqlalchemy import create_engine

# ------------------------------------------------------------
# FUNCION: Obtener los registros nuevos para carga incremental
# ------------------------------------------------------------
def obtener_registros_nuevos(df, clave_primaria, engine, tabla):
    try:
        # Consultar claves primarias existentes en la tabla
        with engine.connect() as conn:
            query = f"SELECT {clave_primaria} FROM {tabla}"
            claves_existentes = pd.read_sql(query, conn)[clave_primaria].tolist()

        # Filtrar registros que no están en la base de datos
        registros_antes = len(df)
        df = df[~df[clave_primaria].isin(claves_existentes)]
        registros_despues = len(df)

        print(f"Se detectaron {registros_despues} registros nuevos para la tabla '{tabla}'.")
    except Exception as e:
        print(f"Error al verificar registros nuevos: {e}")
    return df

# ------------------------------------------------------------
# FUNCION: Validar claves foráneas para evitar errores
# ------------------------------------------------------------
def validar_claves_foraneas(df, columna, tabla_referencia, clave_referencia, engine):
    try:
        # Obtener los valores válidos de la tabla de referencia
        with engine.connect() as conn:
            query = f"SELECT {clave_referencia} FROM {tabla_referencia}"
            valores_validos = pd.read_sql(query, conn)[clave_referencia].tolist()
        
        # Filtrar los registros que tienen claves válidas
        registros_antes = len(df)
        df = df[df[columna].isin(valores_validos)]
        registros_despues = len(df)
        
        print(f"Validación de claves foráneas '{columna}': {registros_despues} registros válidos (de {registros_antes}).")
    except Exception as e:
        print(f"Error al validar claves foráneas para '{columna}': {e}")
    return df

# ------------------------------------------------------------
# FUNCION: Limpiar registros huérfanos en tablas dependientes
# ------------------------------------------------------------
def limpiar_registros_huerfanos(tabla_dependiente, columna_fk, tabla_referencia, columna_referencia, engine):
    try:
        with engine.connect() as conn:
            # Identificar registros huérfanos
            query_huerfanos = f"""
            DELETE FROM {tabla_dependiente}
            WHERE {columna_fk} NOT IN (SELECT {columna_referencia} FROM {tabla_referencia});
            """
            conn.execute(query_huerfanos)
            print(f"Registros huérfanos eliminados en la tabla '{tabla_dependiente}' basados en '{columna_fk}'.")
    except Exception as e:
        print(f"Error al limpiar registros huérfanos en '{tabla_dependiente}': {e}")

# ------------------------------------------------------------
# FUNCION: Cargar archivos CSV a SQL Server (carga incremental)
# ------------------------------------------------------------
def carga_incremental_csv(archivos_csv, conn_str, claves_primarias):
    engine = create_engine(conn_str)
    try:
        for archivo in archivos_csv:
            nombre_tabla = archivo.split('.')[0]  # Nombre de tabla basado en el archivo
            clave_primaria = claves_primarias.get(nombre_tabla, None)
            
            # Leer el archivo CSV
            df = pd.read_csv(archivo)
            
            # Adaptar columnas si es necesario
            if nombre_tabla == "team" and 'id' in df.columns:
                df.rename(columns={'id': 'team_id'}, inplace=True)
            
            # Validar relaciones clave-foránea
            if nombre_tabla == "game":
                df = validar_claves_foraneas(df, 'team_id_home', 'team', 'team_id', engine)
                df = validar_claves_foraneas(df, 'team_id_away', 'team', 'team_id', engine)
            
            if nombre_tabla in ["line_score", "other_stats"]:
                df = validar_claves_foraneas(df, 'game_id', 'game', 'game_id', engine)
                df = validar_claves_foraneas(df, 'team_id_home', 'team', 'team_id', engine)
                df = validar_claves_foraneas(df, 'team_id_away', 'team', 'team_id', engine)
            
            # Verificar registros nuevos en la base de datos
            if clave_primaria:
                df = obtener_registros_nuevos(df, clave_primaria, engine, nombre_tabla)

            # Cargar registros nuevos si existen
            if not df.empty:
                df.to_sql(nombre_tabla, engine, if_exists='append', index=False)
                print(f"Archivo '{archivo}' cargado exitosamente en la tabla '{nombre_tabla}'.")
            else:
                print(f"No se encontraron nuevos registros para cargar en la tabla '{nombre_tabla}'.")
    except Exception as e:
        print(f"Error al cargar el archivo '{archivo}': {e}")

# ------------------------------------------------------------
# Ejecución del script mejorado
# ------------------------------------------------------------
print("Iniciando el proceso de limpieza y carga incremental...")

# Lista de archivos CSV a cargar
archivos_csv = [
    "game.csv",
    "game_info.csv",
    "game_summary.csv",
    "line_score.csv",
    "other_stats.csv",
    "team_details.csv",
    "team_history.csv",
    "team.csv"
]

# Diccionario con las claves primarias de cada tabla
claves_primarias = {
    "game": "game_id",
    "game_info": "game_id",
    "game_summary": "game_id",
    "line_score": "game_id",
    "other_stats": "game_id",
    "team_details": "team_id",
    "team_history": "team_id",
    "team": "team_id"
}

# Cadena de conexión a SQL Server usando SQLAlchemy
conn_str = "mssql+pyodbc://NBA-SERVER-INST\\SQLCODIGOCAMPEON/nba_campeon?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"

# Limpiar registros huérfanos antes de la carga semanal
engine = create_engine(conn_str)
limpiar_registros_huerfanos("line_score", "game_id", "game", "game_id", engine)
limpiar_registros_huerfanos("other_stats", "game_id", "game", "game_id", engine)
limpiar_registros_huerfanos("game_summary", "game_id", "game", "game_id", engine)

# Realizar la carga incremental
carga_incremental_csv(archivos_csv, conn_str, claves_primarias)

print("Proceso de limpieza y carga incremental completo.")


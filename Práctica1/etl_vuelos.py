import pandas as pd
import numpy as np

def extraer_y_limpiar_datos(ruta_archivo):
    print("Iniciando fase de Extracción (E)...")
    try:
        # 1. EXTRACCIÓN
        df = pd.read_csv(ruta_archivo)
        print(f"✅ CSV cargado exitosamente. Filas originales: {len(df)}")
        
        print("Iniciando fase de Transformación y Limpieza (T)...")
        
        # 2. TRANSFORMACIÓN
        
        # a. Estandarización de textos
        print(" -> Estandarizando textos...")
        df['destination_airport'] = df['destination_airport'].astype(str).str.upper()
        df['origin_airport'] = df['origin_airport'].astype(str).str.upper()
        df['passenger_gender'] = df['passenger_gender'].astype(str).str.upper()
        
        # b. Limpieza de números (Arreglando precios con comas ej: "77,60" -> 77.60)
        print(" -> Limpiando formatos numéricos...")
        # Reemplazar coma por punto y convertir a float numérico
        df['ticket_price'] = df['ticket_price'].astype(str).str.replace(',', '.').astype(float)
        
        # c. Tratamiento de Valores Nulos (Manejo de excepciones de negocio)
        print(" -> Tratando valores nulos (Vuelos cancelados)...")
        # Si un vuelo se cancela, su retraso o equipaje facturado puede venir nulo. Lo rellenamos con 0.
        columnas_numericas = ['delay_min', 'duration_min', 'bags_total', 'bags_checked']
        for col in columnas_numericas:
            df[col] = df[col].fillna(0)
            
        # d. Limpieza de Fechas
        print(" -> Estandarizando formatos de fecha...")
        # Coercionamos los errores para que las fechas inválidas se vuelvan NaT (Not a Time)
        df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], format='mixed', errors='coerce')
        df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], format='mixed', errors='coerce')
        
        # Eliminar filas donde la fecha de salida sea nula
        df = df.dropna(subset=['departure_datetime'])
        
        # e. Eliminación de Duplicados
        filas_antes = len(df)
        df = df.drop_duplicates()
        filas_despues = len(df)
        if filas_antes != filas_despues:
            print(f" -> 🧹 Se eliminaron {filas_antes - filas_despues} registros duplicados.")
            
        print("✅ Transformación completada con éxito.")
        return df

    except Exception as e:
        print(f"❌ Error crítico en el proceso ETL: {e}")
        return None

ruta_csv = 'dataset_vuelos_crudo.csv'
df_limpio = extraer_y_limpiar_datos(ruta_csv)

if df_limpio is not None:
    print("\nMuestra de los datos limpios:")
    print(df_limpio[['airline_name', 'destination_airport', 'ticket_price', 'departure_datetime']].head())

import urllib
from sqlalchemy import create_engine

print("\nIniciando fase de Carga (L) a SQL Server...")

# 1. Crear la conexión a SQL Server
# Nota: TrustServerCertificate=yes es crucial en entornos locales de Docker
params = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=localhost,1433;'
    'DATABASE=Practica1_SS2;'
    'UID=sa;'
    'PWD=SuperPassword123!;'
    'TrustServerCertificate=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

try:
    # Probamos la conexión
    conexion = engine.connect()
    print("✅ Conexión exitosa a SQL Server!")
    conexion.close()
    
    # ==========================================
    # CARGA DE DIMENSIÓN AEROLÍNEA
    # ==========================================
    print(" -> Cargando Dim_Aerolinea...")
    
    # a. Extraer valores únicos del DataFrame limpio y renombrar columnas para que coincidan con SQL
    df_dim_aerolinea = df_limpio[['airline_code', 'airline_name']].drop_duplicates().rename(columns={
        'airline_code': 'Codigo_Aerolinea',
        'airline_name': 'Nombre_Aerolinea'
    })
    
    # b. Insertar en SQL Server (index=False es importante, if_exists='append' agrega a la tabla existente)
    df_dim_aerolinea.to_sql('Dim_Aerolinea', engine, if_exists='append', index=False)
    
    # c. Leer la tabla de regreso para obtener las Llaves Subrogadas (IDs generados)
    df_aerolineas_db = pd.read_sql_query("SELECT ID_Aerolinea, Codigo_Aerolinea FROM Dim_Aerolinea", engine)
    
    # d. Hacer MERGE con el DataFrame principal para sustituir los textos por el nuevo ID
    df_limpio = df_limpio.merge(
        df_aerolineas_db, 
        left_on='airline_code', 
        right_on='Codigo_Aerolinea', 
        how='left'
    )
    print("✅ Dim_Aerolinea cargada y conectada en el dataset principal.")

except Exception as e:
    print(f"❌ Error en la base de datos: {e}")    
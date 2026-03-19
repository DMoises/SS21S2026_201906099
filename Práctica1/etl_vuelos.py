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
        
        # Homologación de géneros
        print(" -> Homologando categorías de género...")
        mapeo_genero = {
            'M': 'MASCULINO',
            'F': 'FEMENINO',
            'X': 'NO BINARIO',
            'NOBINARIO': 'NO BINARIO'
        }
        df['passenger_gender'] = df['passenger_gender'].replace(mapeo_genero)

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
# Nota se usó TrustServerCertificate=yes ya que la DB está en Docker
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
    # CARGA DE DIMENSIONES (Corregido contra duplicados)
    # ==========================================
    print(" -> Cargando Dim_Aerolinea...")
    df_dim_aerolinea = df_limpio[['airline_code', 'airline_name']].drop_duplicates(subset=['airline_code']).rename(columns={
        'airline_code': 'Codigo_Aerolinea',
        'airline_name': 'Nombre_Aerolinea'
    })
    df_dim_aerolinea.to_sql('Dim_Aerolinea', engine, if_exists='append', index=False)
    df_aerolineas_db = pd.read_sql_query("SELECT ID_Aerolinea, Codigo_Aerolinea FROM Dim_Aerolinea", engine)
    df_limpio = df_limpio.merge(df_aerolineas_db, left_on='airline_code', right_on='Codigo_Aerolinea', how='left')

    # 1. Dim_Pasajero (Garantizando que un ID de pasajero solo aparezca 1 vez aunque cambie su edad)
    print(" -> Cargando Dim_Pasajero...")
    df_pasajero = df_limpio[['passenger_id', 'passenger_gender', 'passenger_age', 'passenger_nationality']].drop_duplicates(subset=['passenger_id'])
    df_pasajero.rename(columns={'passenger_id': 'Pasajero_ID_Original', 'passenger_gender': 'Genero', 'passenger_age': 'Edad', 'passenger_nationality': 'Nacionalidad'}, inplace=True)
    df_pasajero.to_sql('Dim_Pasajero', engine, if_exists='append', index=False)
    
    df_pasajero_db = pd.read_sql_query("SELECT ID_Pasajero, Pasajero_ID_Original FROM Dim_Pasajero", engine)
    df_limpio = df_limpio.merge(df_pasajero_db, left_on='passenger_id', right_on='Pasajero_ID_Original', how='left')

    # 2. Dim_Aeropuerto
    print(" -> Cargando Dim_Aeropuerto...")
    aeropuertos_unicos = pd.concat([df_limpio['origin_airport'], df_limpio['destination_airport']]).unique()
    df_aeropuerto = pd.DataFrame({'Codigo_Aeropuerto': aeropuertos_unicos}).drop_duplicates()
    df_aeropuerto.to_sql('Dim_Aeropuerto', engine, if_exists='append', index=False)
    
    df_aeropuerto_db = pd.read_sql_query("SELECT ID_Aeropuerto, Codigo_Aeropuerto FROM Dim_Aeropuerto", engine)
    df_limpio = df_limpio.merge(df_aeropuerto_db, left_on='origin_airport', right_on='Codigo_Aeropuerto', how='left')
    df_limpio.rename(columns={'ID_Aeropuerto': 'ID_Aeropuerto_Origen'}, inplace=True)
    df_limpio = df_limpio.merge(df_aeropuerto_db, left_on='destination_airport', right_on='Codigo_Aeropuerto', how='left')
    df_limpio.rename(columns={'ID_Aeropuerto': 'ID_Aeropuerto_Destino'}, inplace=True)

    # 3. Dim_Aeronave
    print(" -> Cargando Dim_Aeronave...")
    df_aeronave = df_limpio[['aircraft_type']].drop_duplicates(subset=['aircraft_type']).rename(columns={'aircraft_type': 'Tipo_Aeronave'})
    df_aeronave.to_sql('Dim_Aeronave', engine, if_exists='append', index=False)
    df_aeronave_db = pd.read_sql_query("SELECT ID_Aeronave, Tipo_Aeronave FROM Dim_Aeronave", engine)
    df_limpio = df_limpio.merge(df_aeronave_db, left_on='aircraft_type', right_on='Tipo_Aeronave', how='left')

    # 4. Dim_Detalle_Boleto (Este usa llave compuesta, así que revisamos las 3 columnas juntas)
    print(" -> Cargando Dim_Detalle_Boleto...")
    df_boleto = df_limpio[['cabin_class', 'sales_channel', 'payment_method']].drop_duplicates(subset=['cabin_class', 'sales_channel', 'payment_method'])
    df_boleto.rename(columns={'cabin_class': 'Clase_Cabina', 'sales_channel': 'Canal_Venta', 'payment_method': 'Metodo_Pago'}, inplace=True)
    df_boleto.to_sql('Dim_Detalle_Boleto', engine, if_exists='append', index=False)
    
    df_boleto_db = pd.read_sql_query("SELECT * FROM Dim_Detalle_Boleto", engine)
    df_limpio = df_limpio.merge(df_boleto_db, left_on=['cabin_class', 'sales_channel', 'payment_method'], right_on=['Clase_Cabina', 'Canal_Venta', 'Metodo_Pago'], how='left')

    # 5. Dim_Status
    print(" -> Cargando Dim_Status...")
    df_status = df_limpio[['status']].drop_duplicates(subset=['status']).rename(columns={'status': 'Estado_Vuelo'})
    df_status.to_sql('Dim_Status', engine, if_exists='append', index=False)
    df_status_db = pd.read_sql_query("SELECT ID_Status, Estado_Vuelo FROM Dim_Status", engine)
    df_limpio = df_limpio.merge(df_status_db, left_on='status', right_on='Estado_Vuelo', how='left')
    # 6. Dim_Tiempo (Generando el ID_Tiempo como número entero YYYYMMDD)
    print(" -> Cargando Dim_Tiempo...")
    df_tiempo = pd.DataFrame()
    df_tiempo['Fecha'] = df_limpio['departure_datetime'].dt.date.drop_duplicates()
    df_tiempo['Fecha'] = pd.to_datetime(df_tiempo['Fecha']) # Aseguramos formato fecha
    df_tiempo['ID_Tiempo'] = df_tiempo['Fecha'].dt.strftime('%Y%m%d').astype(int) # Ej: 20240120
    df_tiempo['Anio'] = df_tiempo['Fecha'].dt.year
    df_tiempo['Mes'] = df_tiempo['Fecha'].dt.month
    df_tiempo['Nombre_Mes'] = df_tiempo['Fecha'].dt.month_name()
    df_tiempo['Dia'] = df_tiempo['Fecha'].dt.day
    df_tiempo['Trimestre'] = df_tiempo['Fecha'].dt.quarter
    df_tiempo['Dia_Semana'] = df_tiempo['Fecha'].dt.day_name()
    
    df_tiempo.to_sql('Dim_Tiempo', engine, if_exists='append', index=False)
    
    # Creamos la misma columna ID_Tiempo temporal en df_limpio para hacer el merge fácilmente
    df_limpio['ID_Tiempo_Temp'] = df_limpio['departure_datetime'].dt.strftime('%Y%m%d').astype(int)
    # En este caso no leemos de la base de datos porque nosotros mismos creamos el ID
    df_limpio['ID_Tiempo_Salida'] = df_limpio['ID_Tiempo_Temp']

    # ==========================================
    # CARGA DE LA TABLA DE HECHOS (FACT_VUELOS)
    # ==========================================
    print("\n -> 🚀 Armado y Carga de Fact_Vuelos...")
    
    # Seleccionar solo las llaves foráneas y las métricas numéricas
    columnas_hechos = [
        'record_id', 'ID_Tiempo_Salida', 'ID_Aerolinea', 'ID_Aeropuerto_Origen', 
        'ID_Aeropuerto_Destino', 'ID_Pasajero', 'ID_Aeronave', 'ID_Detalle_Boleto', 'ID_Status',
        'duration_min', 'delay_min', 'ticket_price_usd_est', 'bags_total', 'bags_checked'
    ]
    
    df_hechos = df_limpio[columnas_hechos].copy()
    
    # Renombrar para que coincida exactamente con las columnas de SQL Server
    df_hechos.rename(columns={
        'record_id': 'Record_ID_Original',
        'duration_min': 'Duracion_Minutos',
        'delay_min': 'Retraso_Minutos',
        'ticket_price_usd_est': 'Precio_Boleto_USD',
        'bags_total': 'Equipaje_Total',
        'bags_checked': 'Equipaje_Documentado'
    }, inplace=True)
    
    df_hechos.to_sql('Fact_Vuelos', engine, if_exists='append', index=False)
    print("✅ ¡Tabla de Hechos cargada exitosamente! ETL FINALIZADO CON ÉXITO 🏆")

except Exception as e:
    print(f"❌ Error en la base de datos: {e}")
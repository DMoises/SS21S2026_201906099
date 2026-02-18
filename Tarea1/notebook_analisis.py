import pandas as pd
import numpy as np

# 1. Carga de Datos [cite: 36]
# Asegúrate de descargar un dataset de Kaggle (ej. ventas, clientes, etc.)
try:
    df = pd.read_csv('data/raw_data.csv')
    print("✅ Dataset cargado correctamente.")
    print(f"Dimensiones originales: {df.shape}")
except FileNotFoundError:
    print("❌ Error: No se encuentra el archivo en data/raw_data.csv")

# --- PASO 1: Exploración Inicial (para el README y "Exploración")  ---
print("\n--- Info Inicial ---")
print(df.info())
print("\n--- Primeras filas ---")
print(df.head())

# --- PASO 2: Limpieza de Datos  ---

# 2.1 Eliminación de Duplicados [cite: 38]
duplicados = df.duplicated().sum()
print(f"\n⚠️ Duplicados detectados: {duplicados}")
if duplicados > 0:
    df.drop_duplicates(inplace=True)
    print(f"✅ Se han eliminado {duplicados} registros duplicados.")

# 2.2 Tratamiento de celdas vacías (Valores Nulos) [cite: 39]
nulos = df.isnull().sum().sum()
print(f"\n⚠️ Valores nulos detectados: {nulos}")

if nulos > 0:
    # ESTRATEGIA: Depende de tu dataset.
    # Ejemplo A: Rellenar numéricos con la media
    # df['columna_numerica'] = df['columna_numerica'].fillna(df['columna_numerica'].mean())
    
    # Ejemplo B: Rellenar categóricos con "Desconocido"
    # df['columna_texto'] = df['columna_texto'].fillna("Desconocido")
    
    # Ejemplo C (Genérico para el lab): Eliminar filas si son pocas, o rellenar (ffill/bfill)
    df.fillna(method='ffill', inplace=True) # Relleno hacia adelante como ejemplo
    print("✅ Valores nulos tratados (relleno forward fill).")

# 2.3 Estandarización de valores y formatos [cite: 40]
# Ejemplo: Convertir columnas de fecha a datetime
# Asegúrate de adaptar esto a TU dataset de Kaggle
# if 'Date' in df.columns:
#     df['Date'] = pd.to_datetime(df['Date'])

# Ejemplo: Normalizar texto (todo a minúsculas para evitar 'Guate' vs 'guate')
# string_columns = df.select_dtypes(include=['object']).columns
# for col in string_columns:
#     df[col] = df[col].str.lower().str.strip()
# print("✅ Formatos de texto y fecha estandarizados.")

# --- PASO 3: Análisis y Visualización (Tablas Pivote/Comparación)  ---
# La rúbrica pide comparar estado original vs depurado.
print("\n--- Dimensiones Finales ---")
print(df.shape)

# Generar una tabla pivote simple para demostrar análisis
# pivot = df.pivot_table(index='Categoria', values='Venta', aggfunc='sum')
# print(pivot)

# 4. Exportación 
df.to_csv('data/clean_data.csv', index=False)
print("\n💾 Dataset limpio guardado en 'data/clean_data.csv'")
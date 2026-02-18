import pandas as pd
import numpy as np

# 1. CARGA DE DATOS
# Asumimos que el archivo está en la misma carpeta o en data/
input_file = 'dataset_sucio.csv' 
output_file = 'dataset_limpio.csv'

try:
    df = pd.read_csv(input_file)
    print(f"✅ Archivo cargado: {df.shape[0]} registros.")
except FileNotFoundError:
    print("❌ Error: No encuentro 'dataset_sucio.csv'. Verifica la ruta.")

# --- PASO 1: LIMPIEZA DE FORMATOS (CRÍTICO para este CSV) ---

# A. Limpiar 'gasto_q': Viene sucio como "467,27" (string con comillas y coma)
# Eliminamos comillas, cambiamos coma por punto y convertimos a número
if 'gasto_q' in df.columns:
    df['gasto_q'] = df['gasto_q'].astype(str).str.replace('"', '', regex=False)
    df['gasto_q'] = df['gasto_q'].str.replace(',', '.', regex=False)
    df['gasto_q'] = pd.to_numeric(df['gasto_q'], errors='coerce') # coerce pone NaN si falla

# B. Limpiar 'fecha_registro': Viene mixto (YYYY-MM-DD y DD/MM/YYYY)
if 'fecha_registro' in df.columns:
    # dayfirst=True ayuda a pandas a entender el formato latino (DD/MM/YYYY)
    df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], dayfirst=True, errors='coerce')

# C. Estandarizar Textos (Espacios extra y mayúsculas/minúsculas)
cols_texto = ['nombre', 'ciudad', 'categoria', 'genero']
for col in cols_texto:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title() # "  guatemala " -> "Guatemala"

# --- PASO 2: GESTIÓN DE NULOS Y DUPLICADOS ---

# A. Eliminar Duplicados (Basado en ID o fila completa)
duplicados = df.duplicated().sum()
df.drop_duplicates(inplace=True)
print(f"🗑️ Se eliminaron {duplicados} filas duplicadas.")

# B. Llenar Vacíos (Imputación simple)
# Gasto: Llenar con la media
media_gasto = df['gasto_q'].mean()
df['gasto_q'] = df['gasto_q'].fillna(media_gasto)

# Otros campos texto: Llenar con "Desconocido"
df.fillna("Desconocido", inplace=True)

print(f"✅ Nulos tratados. Gasto promedio imputado: {media_gasto:.2f}")

# --- PASO 3: EXPORTACIÓN ---
df.to_csv(output_file, index=False)
print(f"💾 Archivo limpio guardado como: {output_file}")

# --- VALIDACIÓN FINAL (Para tu reporte) ---
print("\n--- Muestra Final ---")
print(df.head())
print(df.info())
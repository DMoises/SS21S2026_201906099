import pandas as pd
import os

# Rutas relativas para que funcione en cualquier PC
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, 'data', 'dataset_sucio.csv')
output_path = os.path.join(base_dir, 'data', 'clean_data.csv')

print(f"📂 Leyendo archivo desde: {input_path}")

try:
    df = pd.read_csv(input_path)
except FileNotFoundError:
    print("❌ ERROR: No encuentro 'dataset_sucio.csv' en la carpeta data/.")
    exit()

# --- 1. LIMPIEZA DE FORMATOS ---
# Limpiar 'gasto_q': Quitar comillas, cambiar coma por punto
if 'gasto_q' in df.columns:
    df['gasto_q'] = df['gasto_q'].astype(str).str.replace('"', '', regex=False)
    df['gasto_q'] = df['gasto_q'].str.replace(',', '.', regex=False)
    df['gasto_q'] = pd.to_numeric(df['gasto_q'], errors='coerce')

# Limpiar fechas
if 'fecha_registro' in df.columns:
    df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], dayfirst=True, errors='coerce')

# Limpiar textos (Espacios y Capitalización)
cols_texto = ['nombre', 'ciudad', 'categoria', 'genero']
for col in cols_texto:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()

# --- 2. DUPLICADOS Y NULOS ---
df.drop_duplicates(inplace=True)
df['gasto_q'] = df['gasto_q'].fillna(df['gasto_q'].mean())
df.fillna("Desconocido", inplace=True)

# --- 3. EXPORTAR ---
df.to_csv(output_path, index=False)
print(f"✅ ¡Éxito! Archivo limpio creado en: {output_path}")
print("--- Muestra final ---")
print(df.head())
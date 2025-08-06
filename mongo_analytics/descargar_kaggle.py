import os
import sys
import subprocess
import glob

# -------- Paso 1: Instalar dependencias si faltan --------
def instalar_paquete(paquete):
    try:
        __import__(paquete)
    except ImportError:
        print(f"📦 Instalando '{paquete}'...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", paquete])

instalar_paquete("kaggle")
instalar_paquete("pandas")

from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

# -------- Paso 2: Verificar kaggle.json --------
kaggle_json_path = os.path.expanduser("~/.kaggle/kaggle.json")
if not os.path.exists(kaggle_json_path):
    print(f"❌ No se encontró el archivo {kaggle_json_path}")
    print("➡️ Descarga tu API token desde: https://www.kaggle.com/settings")
    sys.exit(1)

# -------- Paso 3: Descargar dataset desde Kaggle --------
api = KaggleApi()
api.authenticate()

dataset_name = 'mkechinov/ecommerce-purchase-history-from-electronics-store'
destino = 'datos_kaggle'

os.makedirs(destino, exist_ok=True)

print("⬇️ Descargando el dataset desde Kaggle...")
api.dataset_download_files(dataset_name, path=destino, unzip=True)
print(f"✅ Dataset descargado en: {destino}")

# -------- Paso 4: Buscar automáticamente el archivo CSV --------
print(f"\n🔍 Buscando archivos CSV en '{destino}'...")
csv_files = glob.glob(os.path.join(destino, "*.csv"))

if not csv_files:
    print("❌ No se encontró ningún archivo CSV en la carpeta.")
    sys.exit(1)

csv_path = csv_files[0]
print(f"\n🧾 Archivo CSV encontrado: {csv_path}")

# -------- Paso 5: Cargar el archivo con pandas --------
df = pd.read_csv(csv_path)
print("\n✅ Primeras filas del dataset:\n")
print(df.head())

# -------- Paso 6: Rellenar o eliminar valores nulos en 'Brand' --------
if 'Brand' in df.columns:
    print("\n🛠️ Rellenando valores nulos en 'Brand' con 'Unknown'...\n")
    df['Brand'] = df['Brand'].fillna('Unknown')  # También podrías usar df.dropna(subset=['Brand'])

# -------- Paso 7: Análisis básico --------
print("\n📋 Info general:\n")
print(df.info())

print("\n📊 Estadísticas generales:\n")
print(df.describe(include='all'))

print("\n🚨 Columnas con valores nulos:\n")
print(df.isnull().sum())

if 'Brand' in df.columns:
    print("\n🏷️ Top 10 marcas:\n")
    print(df['Brand'].value_counts().head(10))

# -------- Paso 8: Guardar la versión limpia --------
output_path = os.path.join(destino, "kz_cleaned.csv")
df.to_csv(output_path, index=False)
print(f"\n💾 Archivo limpio guardado como: {output_path}")

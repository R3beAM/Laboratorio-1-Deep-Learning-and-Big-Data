import os
import subprocess
import sys

# -------- Paso 1: Instalar kaggle y pandas si faltan --------
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

# -------- Paso 3: Descargar el dataset --------
api = KaggleApi()
api.authenticate()

dataset_name = 'mkechinov/ecommerce-purchase-history-from-electronics-store'
destino = 'datos_kaggle'

os.makedirs(destino, exist_ok=True)

print("⬇️ Descargando el dataset desde Kaggle...")
api.dataset_download_files(dataset_name, path=destino, unzip=True)
print(f"✅ Dataset descargado en: {destino}")

# -------- Paso 4: Cargar y analizar 'kz.csv' --------
csv_path = os.path.join(destino, "kz.csv")

if os.path.exists(csv_path):
    print(f"\n🧾 Leyendo 'kz.csv'...\n")
    df = pd.read_csv(csv_path)

    # Rellenar nulos en 'Brand'
if 'Brand' in df.columns:
    print("\n🛠️ Rellenando valores nulos en 'Brand' con 'Unknown'...\n")
    df['Brand'] = df['Brand'].fillna('Unknown')

    # -------- Análisis básico --------
    print("\n📋 Primeras filas del dataset:\n")
    print(df.head())

    print("\n🔍 Información general:\n")
    print(df.info())

    print("\n📊 Estadísticas generales:\n")
    print(df.describe(include='all'))

    print("\n🚨 Columnas con valores nulos:\n")
    print(df.isnull().sum())

    # Análisis por columnas clave si existen
    for col in ['category', 'product_name', 'payment_method']:
        if col in df.columns:
            print(f"\n📦 Conteo de valores únicos en '{col}':\n")
            print(df[col].value_counts().head(10))
    
    df.to_csv(os.path.join(destino, 'kz_cleaned.csv'), index=False)
    print("✅ Archivo limpio guardado como 'kz_cleaned.csv'")
    
else:
    print("❌ No se encontró el archivo 'kz.csv' en la carpeta descargada.")

    

import os
import subprocess
import sys

# -------- Paso 1: Verificar e instalar kaggle --------
try:
    from kaggle.api.kaggle_api_extended import KaggleApi
except ImportError:
    print("Paquete 'kaggle' no encontrado. Instalando...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle"])
    from kaggle.api.kaggle_api_extended import KaggleApi

# -------- Paso 2: Verificar que kaggle.json exista --------
kaggle_json_path = os.path.expanduser("~/.kaggle/kaggle.json")
if not os.path.exists(kaggle_json_path):
    print(f"❌ No se encontró el archivo {kaggle_json_path}")
    print("➡️ Descarga tu API token desde: https://www.kaggle.com/settings")
    print("y colócalo en la ruta anterior.")
    sys.exit(1)

# -------- Paso 3: Autenticarse y descargar el dataset --------
api = KaggleApi()
api.authenticate()

dataset_name = 'mkechinov/ecommerce-purchase-history-from-electronics-store'
destino = 'datos_kaggle'

# Crear carpeta si no existe
os.makedirs(destino, exist_ok=True)

print("📦 Descargando el dataset desde Kaggle...")
api.dataset_download_files(dataset_name, path=destino, unzip=True)

print(f"✅ Dataset descargado y descomprimido en: {destino}")



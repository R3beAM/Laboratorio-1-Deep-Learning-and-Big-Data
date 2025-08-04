kaggle
import os
from kaggle.api.kaggle_api_extended import KaggleApi

# Autenticación
api = KaggleApi()
api.authenticate()

# Descarga del dataset completo y descompresión
dataset_name = 'mkechinov/ecommerce-purchase-history-from-electronics-store'
destino = 'datos_kaggle'  # carpeta donde se guardará

# Crear carpeta si no existe
os.makedirs(destino, exist_ok=True)

# Descargar y descomprimir
api.dataset_download_files(dataset_name, path=destino, unzip=True)

print(f"Dataset descargado en la carpeta: {destino}")


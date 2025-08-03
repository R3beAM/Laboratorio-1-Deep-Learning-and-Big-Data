pip install pandas
import pandas as pd

# Cambia la ruta por la ubicación real de tu archivo
ruta_csv = r"C:\Usuarios\TuNombreUsuario\Descargas\mis_datos.csv"

# Lee el archivo CSV
df = pd.read_csv(ruta_csv)

# Muestra las primeras filas
print(df.head())

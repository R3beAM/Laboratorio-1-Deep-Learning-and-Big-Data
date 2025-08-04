mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
kaggle datasets download -d mkechinov/ecommerce-purchase-history-from-electronics-store
unzip ecommerce-purchase-history-from-electronics-store.zip
pip install pandas
import pandas as pd

df = pd.read_csv("kz.csv")
print(df.head())

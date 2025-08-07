from pymongo import MongoClient

# Conectar a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['kz_database']
collection = db['kz_collection']

# Recuperar todos los documentos de la colección
documentos = collection.find().limit(10)  # Limitar a los primeros 10 documentos

# Imprimir cada documento
for doc in documentos:
    print(doc)

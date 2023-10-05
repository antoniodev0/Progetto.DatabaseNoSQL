from pymongo import MongoClient
import pandas as pd

total_transactions = 30000

# Partiziono il dataset completo
num_transactions = [int(total_transactions * pct) for pct in [1.0, 0.75, 0.50, 0.25]]

client = MongoClient('mongodb://localhost:27017/')
db = client['antiriciclaggio']

# Definisci le entità da trattare (transazioni, mittenti e destinatari)
entities = ['transazioni', 'mittenti', 'destinatari']

for entity in entities:
    for idx, num in enumerate(num_transactions):
        collection_name = f'{entity}_{int((num / total_transactions) * 100)}%'
        collection = db[collection_name]

        # Leggi il file CSV appropriato
        csv_filename = f'{entity}_{int((num / total_transactions) * 100)}%.csv'
        data = pd.read_csv(csv_filename)

        # Converto il dataset in formato JSON
        data_json = data.to_dict(orient='records')

        # Inserisco i dati nel database
        collection.insert_many(data_json)

        print(f"Dati {entity} inseriti in MongoDB con successo per la percentuale {int((num / total_transactions) * 100)}%.")

print("Tutti i dati inseriti in MongoDB con successo.")

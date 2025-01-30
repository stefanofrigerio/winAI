import os
import pandas as pd
from google.cloud import firestore

def load_data_to_firestore(directory_path):
    # Configura il client Firestore per l'emulatore locale
    os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:5000"
    os.environ["GOOGLE_CLOUD_PROJECT"] = "demo-project"

    # Inizializza il client Firestore
    db = firestore.Client()

    # Scorri tutti i file nella directory specificata
    for filename in os.listdir(directory_path):
        if filename.endswith('_dump.csv'):
            # Estrai il nome della collezione dal nome del file
            collection_name = filename.split('_dump')[0]

            # Leggi il file CSV
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)

            # Carica i dati su Firestore
            for index, row in df.iterrows():
                doc_ref = db.collection(collection_name).document()
                doc_ref.set(row.to_dict())
            print(f"Dati caricati con successo nella collezione '{collection_name}'.")

if __name__ == "__main__":
    directory_path = 'database_dump'
    load_data_to_firestore(directory_path) 
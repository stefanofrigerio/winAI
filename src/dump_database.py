import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import os
from datetime import datetime


def initialize_firestore():
    """Inizializza la connessione a Firestore locale."""
    try:
        # Configura l'endpoint dell'emulatore e il project ID
        os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:5000"
        os.environ["GOOGLE_CLOUD_PROJECT"] = "demo-project"

        # Crea le credenziali per l'emulatore
        cred = credentials.ApplicationDefault()

        # Inizializza l'app con il project ID specifico
        firebase_admin.initialize_app(cred, {
            'projectId': 'demo-project',
        })

        return firestore.client()
    except Exception as e:
        print(f"Errore durante l'inizializzazione di Firestore: {e}")
        return None


def get_all_collections(db):
    """Recupera tutte le collections dal database."""
    try:
        collections = db.collections()
        return [collection.id for collection in collections]
    except Exception as e:
        print(f"Errore durante il recupero delle collections: {e}")
        return []


def export_collection_to_csv(db, collection_name, output_dir):
    """Esporta una singola collection in un file CSV."""
    try:
        # Crea la directory se non esiste
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Recupera tutti i documenti della collection
        docs = db.collection(collection_name).stream()

        # Converti i documenti in una lista di dizionari
        data = []
        for doc in docs:
            doc_dict = doc.to_dict()
            doc_dict['document_id'] = doc.id  # Aggiungi l'ID del documento
            data.append(doc_dict)

        if not data:
            print(f"La collection {collection_name} è vuota")
            return

        # Crea il DataFrame
        df = pd.DataFrame(data)

        # Genera il nome del file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{collection_name}_dump_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)

        # Esporta in CSV
        df.to_csv(filepath, index=False)
        print(f"Collection {collection_name} esportata in {filepath}")

    except Exception as e:
        print(f"Errore durante l'esportazione della collection {collection_name}: {e}")


def main():
    # Inizializza Firestore
    db = initialize_firestore()
    if not db:
        return

    # Directory di output
    output_dir = "database_dump"

    # Recupera tutte le collections
    collections = get_all_collections(db)

    if not collections:
        print("Nessuna collection trovata nel database")
        return

    # Esporta ogni collection
    for collection in collections:
        export_collection_to_csv(db, collection, output_dir)

    print("\nEsportazione completata!")


if __name__ == "__main__":
    main()
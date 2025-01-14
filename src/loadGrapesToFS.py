import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from typing import List, Dict, Any
import os


class FirestoreLoader:
    def __init__(self, csv_path: str, collection_name: str = 'grapes', array_fields: List[str] = None):
        """
        Inizializza il loader per Firestore.
        """
        os.environ["FIRESTORE_EMULATOR_HOST"] = "127.0.0.1:5000"
        os.environ["GOOGLE_CLOUD_PROJECT"] = "demo-project"

        # Pulisci eventuali istanze precedenti
        for app in firebase_admin._apps.keys():
            firebase_admin.delete_app(firebase_admin.get_app(app))

        firebase_admin.initialize_app(options={
            'projectId': 'demo-project',
        })

        self.db = firestore.client()
        self.collection_name = collection_name
        self.csv_path = csv_path
        self.array_fields = array_fields or []
        print("Inizializzazione completata")

    def read_csv(self) -> List[Dict[str, Any]]:
        """
        Legge il CSV e lo converte in una lista di dizionari.
        """
        df = pd.read_csv(self.csv_path, na_values=['', 'None', 'none', 'NONE'])
        df = df.dropna(subset=['grape'])
        df['grape'] = df['grape'].astype(str)
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        data = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                if pd.notna(value):
                    # Se il campo è nella lista array_fields, convertilo in array
                    if col in self.array_fields:
                        record[col] = self.convert_to_array(str(value))
                    else:
                        record[col] = str(value).strip()
            if record:
                data.append(record)

        print(f"Letti {len(data)} record validi dal CSV")
        # Mostra un esempio di record convertito
        if data:
            print("\nEsempio di record convertito:")
            print(data[0])
        return data

    def upload_to_firestore(self) -> None:

        """erase the collection"""
        """
        Carica i dati su Firestore.
        """
        try:
            data = self.read_csv()
            if not data:
                print("Nessun dato da caricare")
                return

            print(f"Caricamento di {len(data)} documenti...")
            collection = self.db.collection(self.collection_name)

            # Carica i documenti uno alla volta invece di usare batch
            successful = 0
            failed = 0

            for i, record in enumerate(data):
                try:
                    doc_id = record['grape'].lower()
                    doc_id = ''.join(c if c.isalnum() or c == '_' else '_' for c in doc_id)

                    # Carica direttamente il documento
                    collection.document(doc_id).set(record)
                    successful += 1

                    if (i + 1) % 50 == 0:
                        print(f"Progresso: {i + 1}/{len(data)} documenti processati")

                except Exception as e:
                    print(f"Errore nel caricamento del documento {i} (ID: {doc_id}): {str(e)}")
                    failed += 1

            print(f"\nCaricamento completato:")
            print(f"- Documenti caricati con successo: {successful}")
            print(f"- Documenti falliti: {failed}")

        except Exception as e:
            print(f"Errore durante il caricamento: {str(e)}")

    def verify_upload(self) -> None:
        """
        Verifica che i documenti siano stati caricati.
        """
        try:
            print("\nVerifica dei documenti caricati...")
            collection = self.db.collection(self.collection_name)
            docs = list(collection.get())

            print(f"Trovati {len(docs)} documenti nel database")

            if docs:
                print("\nPrimi 5 documenti caricati:")
                for doc in docs[:5]:
                    print(f"- ID: {doc.id}")
                    print(f"  Dati: {doc.to_dict()}")
                    print()

        except Exception as e:
            print(f"Errore durante la verifica: {str(e)}")

    def convert_to_array(self, value: str) -> List[str]:
        """
        Converte una stringa separata da virgole in un array di stringhe.

        Args:
            value: Stringa separata da virgole

        Returns:
            Lista di stringhe pulite
        """
        if pd.isna(value) or not value:
            return []
        return [item.strip() for item in value.split(',') if item.strip()]

if __name__ == "__main__":
    csv_path = "../output_data/registro_formatted.csv"
    array_fields = ['known_also_as']

    if not os.path.exists(csv_path):
        print(f"File non trovato: {csv_path}")
        exit(1)

    print("=== INIZIO PROCESSO DI CARICAMENTO ===")
    loader = FirestoreLoader(csv_path, array_fields=array_fields)
    loader.upload_to_firestore()
    loader.verify_upload()
    print("=== PROCESSO COMPLETATO ===")
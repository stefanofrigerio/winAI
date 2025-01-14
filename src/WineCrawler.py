import firebase_admin
from firebase_admin import credentials, firestore
import requests
from bs4 import BeautifulSoup
import time
import os
from typing import Optional, Dict, Any


class WineCrawler:
    def __init__(self, collection_name: str = 'grapes'):
        """
        Inizializza il crawler e la connessione a Firestore.
        """
        os.environ["FIRESTORE_EMULATOR_HOST"] = "127.0.0.1:5000"
        os.environ["GOOGLE_CLOUD_PROJECT"] = "demo-project"

        if not firebase_admin._apps:
            firebase_admin.initialize_app(options={
                'projectId': 'demo-project',
            })

        self.db = firestore.client()
        self.collection_name = collection_name
        self.base_url = "https://www.quattrocalici.it/vitigni/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_wine_characteristics(self, grape_name: str) -> Optional[str]:
        """
        Estrae le caratteristiche del vino dalla pagina web.

        Args:
            grape_name: Nome del vitigno

        Returns:
            Testo con le caratteristiche del vino o None se non trovato
        """
        try:
            # Normalizza il nome dell'uva per l'URL
            url_grape = grape_name.lower().replace(' ', '-')
            url = f"{self.base_url}{url_grape}"

            print(f"Analisi URL: {url}")

            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Cerca la sezione delle caratteristiche
            characteristics_section = soup.find('h5', text='Caratteristiche del vino')
            if characteristics_section:
                # Trova il paragrafo successivo che contiene le caratteristiche
                # Manteniamo gli spazi originali tra le parole usando .stripped_strings
                td_content = characteristics_section.find_parent('td')
                if td_content:
                    # Rimuovi l'header "Caratteristiche del vino"
                    h5_tag = td_content.find('h5')
                    if h5_tag:
                        h5_tag.decompose()

                    # Ottieni il testo mantenendo la formattazione
                    text_parts = []
                    for string in td_content.stripped_strings:
                        text_parts.append(string)

                    # Unisci le parti con spazi
                    text = ' '.join(text_parts)
                    return text.strip() if text else None

            return None

        except requests.RequestException as e:
            print(f"Errore nel recupero della pagina per {grape_name}: {str(e)}")
            return None
        except Exception as e:
            print(f"Errore generico per {grape_name}: {str(e)}")
            return None

    def update_firestore(self, grape: str, characteristics: Optional[str]) -> None:
        """
        Aggiorna il documento su Firestore con le caratteristiche del vino.

        Args:
            grape: Nome del vitigno
            characteristics: Caratteristiche del vino o None se non trovate
        """
        try:
            doc_id = grape.lower()
            doc_id = ''.join(c if c.isalnum() or c == '_' else '_' for c in doc_id)

            doc_ref = self.db.collection(self.collection_name).document(doc_id)

            # Se abbiamo trovato le caratteristiche, le aggiorniamo
            # Se non le abbiamo trovate, settiamo esplicitamente il campo a None
            update_data = {
                'tasting_grape': characteristics if characteristics else None
            }

            doc_ref.update(update_data)

            if characteristics:
                print(f"Aggiornato documento per {grape} con caratteristiche")
            else:
                print(f"Impostato tasting_grape a null per {grape}")

        except Exception as e:
            print(f"Errore nell'aggiornamento di Firestore per {grape}: {str(e)}")

    def process_grapes(self, limit: int = 10) -> None:
        """
        Processa i primi N vitigni dal database.

        Args:
            limit: Numero massimo di vitigni da processare
        """
        try:
            # Recupera i documenti da Firestore
            collection_ref = self.db.collection(self.collection_name)
            query = collection_ref.limit(limit) if limit else collection_ref
            docs = query.get()

            for doc in docs:
                data = doc.to_dict()
                grape = data.get('grape')

                if not grape:
                    continue

                print(f"\nProcessing: {grape}")
                characteristics = self.get_wine_characteristics(grape)
                self.update_firestore(grape, characteristics)

                # Pausa per evitare di sovraccaricare il server
                time.sleep(2)

        except Exception as e:
            print(f"Errore durante il processing: {str(e)}")


if __name__ == "__main__":
    print("=== INIZIO CRAWLING ===")
    crawler = WineCrawler()
    crawler.process_grapes(limit=10)  # Processa i primi 10 vitigni
    print("=== CRAWLING COMPLETATO ===")
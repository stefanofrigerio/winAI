import streamlit as st
import os
from google.cloud import firestore
from ai_model_initializer import AIModelInitializer

# Inizializza il client Firestore
os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:5000"
os.environ["GOOGLE_CLOUD_PROJECT"] = "demo-project"
db = firestore.Client()

def get_grapes():
    # Recupera tutti i documenti dalla collezione 'grapes'
    grapes_ref = db.collection('grapes')
    docs = grapes_ref.stream()

    # Filtra i documenti in cui 'tasting_grape' non è nullo e estrai i valori unici del campo 'grape'
    grapes = {doc.to_dict().get('grape') for doc in docs if doc.to_dict().get('tasting_grape') is not None and doc.to_dict().get('grape')}
    return list(grapes)

def main():
    st.title("Seleziona un Vitigno")

    # Ottieni l'elenco dei vitigni
    grapes = get_grapes()

    # Crea un elenco a scomparsa
    selected_grape = st.selectbox("Scegli un vitigno:", grapes)

    # Bottone per inviare il prompt
    if st.button("Invia"):
        if selected_grape:
            # Inizializza il modello AI
            ai_initializer = AIModelInitializer()
            prompt = f"voglio un elenco delle principali etichette che usano il vitigno {selected_grape}"
            response = ai_initializer.get_model_response(prompt)
            
            # Mostra la risposta
            st.write("Risposta del modello AI:")
            st.write(response)

if __name__ == "__main__":
    main() 
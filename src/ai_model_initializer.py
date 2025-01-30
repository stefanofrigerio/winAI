import os
from openai import OpenAI

class AIModelInitializer:
    def __init__(self):
        # Ottieni la chiave API dall'ambiente
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("La chiave API non è stata trovata. Assicurati che OPENAI_API_KEY sia impostata.")

        # Configura la chiave API per OpenAI
        self.client = OpenAI(api_key=self.api_key)

    def get_model_response(self, prompt, model="gpt-4o-mini"):
        try:
            # Richiama il modello AI con il prompt fornito usando la nuova API
            response = self.client.chat.completions.create(model=model,
            messages=[
                {"role": "system", "content": "Sei un assistente utile."},
                {"role": "user", "content": prompt}
            ])
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Errore durante l'interazione con il modello: {e}")
            return None 
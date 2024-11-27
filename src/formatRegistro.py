import pandas as pd

import pandas as pd

def format_registro():
    # Leggi il file CSV
    df = pd.read_csv('../input_data/Export_registro.csv')
    
    # Crea un DataFrame con valori distinct della colonna Varietà
    df_distinct = df[['Varieta', 'Sinonimi']].drop_duplicates()
    
    # Crea il nuovo DataFrame con le colonne richieste
    df_output = pd.DataFrame()
    
    # Estrai il colore e pulisci la varietà
    df_output['grape'] = df_distinct['Varieta'].str.replace(r'(B\.|N\.)\s*', '', regex=True).str.strip()
    df_output['colour'] = df_distinct['Varieta'].str.extract(r'(B\.|N\.)', expand=False).str.replace('.', '')
   
    df_output['known_also_as'] = df_distinct['Sinonimi']
    df_output = df_output.drop_duplicates(subset=['grape'], keep='first')
    # Rimuovi i numeri tra parentesi e gli asterischi
    df_output['known_also_as'] = df_output['known_also_as'].str.replace(r'\(\d+\)\s*', '', regex=True)  # rimuove (266) 
    df_output['known_also_as'] = df_output['known_also_as'].str.replace('*', '', regex=False)  # rimuove asterischi
    df_output['known_also_as'] = df_output['known_also_as'].str.replace(r'B\.\s*', 'BIANCO', regex=True).str.strip()
    df_output['known_also_as'] = df_output['known_also_as'].str.replace(r'N\.\s*', 'NERO', regex=True).str.strip()
    df_output['known_also_as'] = df_output['known_also_as'].astype(str).str.replace('"', '')
    df_output['known_also_as'] = df_output['known_also_as'].fillna('')
    df_output['known_also_as'] = df_output['known_also_as'].replace('nan', '')
    # Salva il risultato in un nuovo file
    df_output.to_csv('../output_data/registro_formatted.csv', index=False)
    
    return df_output

if __name__ == "__main__":
    format_registro()
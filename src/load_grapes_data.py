import os

import pandas as pd


def format_registro(input_path, output_path):
    """Formats the Export_registro.csv file to clean grape varieties and synonyms.

    Args:
        input_path (str): Path to the input CSV file.
        output_path (str): Path to save the formatted CSV file.

    Returns:
        pandas.DataFrame: The formatted DataFrame.
    """
    df = pd.read_csv(input_path)
    df_distinct = df[['Varieta', 'Sinonimi']].drop_duplicates()

    df_output = pd.DataFrame()
    df_output['grape'] = df_distinct['Varieta'].str.replace(r'(B\.|N\.)\s*', '', regex=True).str.strip()
    df_output['colour'] = df_distinct['Varieta'].str.extract(r'(B\.|N\.)', expand=False).str.replace('.', '')
    df_output['known_also_as'] = df_distinct['Sinonimi'].fillna('')

    replacements = [
        (r'\(\d+\)\s*', ''),
        ('*', ''),
        (r'B\.\s*', 'BIANCO'),
        (r'N\.\s*', 'NERO'),
        ('"', '')
    ]
    for pattern, replacement in replacements:
        df_output['known_also_as'] = df_output['known_also_as'].str.replace(pattern, replacement,
                                                                            regex=True if pattern.startswith(
                                                                                'r') else False)

    df_output = df_output.drop_duplicates(subset=['grape'], keep='first')
    df_output.to_csv(output_path, index=False)
    return df_output


if __name__ == "__main__":
    input_path = os.path.join('..', 'input_data', 'Export_registro.csv')
    output_path = os.path.join("..", "output_data", 'registro_formatted.csv')
    format_registro(input_path, output_path)

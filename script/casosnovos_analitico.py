import pandas as pd
import oracledb
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

### EXTRAÇÃO

# Function to extract year and month from filename
def extract_year_month(filename):
    # Extract year and month from the format 'CasosNovos_YYYY_MM'
    parts = filename.split('_')
    if len(parts) == 3:
        year = parts[1]
        month = parts[2].split('.')[0]  # Remove .csv extension from the month part
        return year, month
    else:
        raise ValueError("Filename format should be 'CasosNovos_YYYY_MM.csv'")

# Path to the folder containing CSV files
folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Casos Novos\\ejud' 
print(f'Folder Path: {folder_path}')

# Initialize an empty DataFrame to store all the data
MPM_CASOSNOVOS_ANALITICO = pd.DataFrame()

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        # Extract year and month from the filename
        year, month = extract_year_month(filename)
        
        # Full path to the current CSV file
        csv_file_path = os.path.join(folder_path, filename)

        print(f'CSV Path: {csv_file_path}')

                # Check if file exists before reading
        if os.path.exists(csv_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(csv_file_path, encoding='ISO-8859-1', delimiter = ';')
                print(f"Data from {filename} loaded successfully!")
                # Additional code to handle df (e.g., adding year/month columns, processing, etc.)
                
                # Add 'year' and 'month' columns to the DataFrame
                df['year'] = year
                df['month'] = month
                
                # Append the data from this file to the main DataFrame
                MPM_CASOSNOVOS_ANALITICO = pd.concat([MPM_CASOSNOVOS_ANALITICO, df], ignore_index=True)

            except Exception as e:
                print(f"Error reading {csv_file_path}: {e}")
        else:
            print(f"File not found: {csv_file_path}")
          
MPM_CASOSNOVOS_ANALITICO.columns.values[0:14] = ['CNJ', 'NUMERO_ANTIGO', 'PRIMEIRA_FASE', 'RELATOR', 'EX_RELATOR', 
                           'ORGAO_JULGADOR', 'DATA_FASE', 'FASE', 'CLASSE_CNJ', 'NATUREZA', 
                           'TIPO', 'ELETRONICO', 'ANO', 'MES']

### CONFIGURANDO CONEXÃO

oracle_dsn_config = {
    'host': os.getenv('ORACLE_DB_HOST'),
    'port': os.getenv('ORACLE_DB_PORT'),
    'service_name': os.getenv('ORACLE_DB_SERVICE_NAME')
}

oracle_dsn = oracledb.makedsn(host=oracle_dsn_config['host'],port=oracle_dsn_config['port'],service_name=oracle_dsn_config['service_name'])

oracle_config = {
    'user': os.getenv("ORACLE_DB_USER"),
    'password': os.getenv("ORACLE_DB_PASSWORD"),
    'dsn': oracle_dsn
}

# Insert each row into the Oracle table
conn = None

try:
    logging.info("Conectando-se ao banco de dados Oracle.")
    conn = oracledb.connect(**oracle_config)
    cursor = conn.cursor()

    cursor.execute('DELETE FROM DW_DEIGE.MPM_CASOSNOVOS_ANALITICO')

    # Insert data into Oracle table row by row
    for index, row in MPM_CASOSNOVOS_ANALITICO.iterrows():
        cursor.execute(
            """
            INSERT INTO DW_DEIGE.MPM_CASOSNOVOS_ANALITICO
            (CNJ, NUMERO_ANTIGO, PRIMEIRA_FASE, RELATOR, EX_RELATOR, ORGAO_JULGADOR, DATA_FASE, FASE, CLASSE_CNJ, NATUREZA, TIPO, ELETRONICO, ANO, MES) 
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
            """, 
            (row['CNJ'], row['NUMERO_ANTIGO'], row['PRIMEIRA_FASE'], row['RELATOR'], row['EX_RELATOR'], row['ORGAO_JULGADOR'], 
            row['DATA_FASE'], row['FASE'], row['CLASSE_CNJ'], row['NATUREZA'], row['TIPO'], row['ELETRONICO'], row['ANO'], row['MES'])
        )
    # Commit the transaction
    conn.commit()
    logging.info("Dados inseridos com sucesso.")

except oracledb.Error as e:
    logging.error(f"Falha ao se conectar ou inserir dados no banco de dados Oracle: {e}")
finally:
    # Ensure the cursor and connection are closed properly
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logging.info("Conexão com o banco de dados Oracle foi fechada.")
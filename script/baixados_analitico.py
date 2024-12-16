import pandas as pd
import oracledb
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[
        logging.StreamHandler(),  # Output logs to the console
        logging.FileHandler("app.log")  # Also log to a file
    ]
)

### EXTRAÇÃO

#Initialize an empty DataFrame to store all the data
MPM_BAIXADOS_ANALITICO = pd.DataFrame()

# Function to extract year and month from filename
def extract_year_month(filename):
    # Extract year and month from the format 'CasosNovos_YYYY_MM'
    parts = filename.split('_')
    if len(parts) == 3:
        year = parts[1]
        month = parts[2].split('.')[0]  # Remove .csv extension from the month part
        return year, month
    else:
        raise ValueError("Filename format should be 'Baixados_YYYY_MM.csv'")

# Path to the folder containing CSV files
folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Baixados\\ejud' 
print(f'Folder Path: {folder_path}')

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
                MPM_BAIXADOS_ANALITICO = pd.concat([MPM_BAIXADOS_ANALITICO, df], ignore_index=True)

            except Exception as e:
                print(f"Error reading {csv_file_path}: {e}")
        else:
            print(f"File not found: {csv_file_path}")

 # Use only the first 9 columns
            MPM_BAIXADOS_ANALITICO = MPM_BAIXADOS_ANALITICO.iloc[:, :9]

MPM_BAIXADOS_ANALITICO.columns.values[0:11] = ['NUMERO_ANTIGO','CNJ','CLASSE_CNJ','NATUREZA','DATA_DA_BAIXA',
                                               'MAG_ULT_CONCLUSAO','ORGAO_JULGADOR','RELATOR','OJ',
                                               'ANO','MES']

# Convert 'date_column' to datetime
MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'] = MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'].str.split(' ').str[0]
MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'] = pd.to_datetime(MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'], format='%d/%m/%Y')

#MPM_BAIXADOS_ANALITICO['DATA_DE_AUTUACAO'] = MPM_BAIXADOS_ANALITICO['DATA_DE_AUTUACAO'].str.split(' ').str[0]
#MPM_BAIXADOS_ANALITICO['DATA_DE_AUTUACAO'] = pd.to_datetime(MPM_BAIXADOS_ANALITICO['DATA_DE_AUTUACAO'], format='%d/%m/%Y')

#Tratando nulos
MPM_BAIXADOS_ANALITICO['MAG_ULT_CONCLUSAO'] = MPM_BAIXADOS_ANALITICO['MAG_ULT_CONCLUSAO'].fillna(0).astype(int)
MPM_BAIXADOS_ANALITICO['ORGAO_JULGADOR'] = MPM_BAIXADOS_ANALITICO['ORGAO_JULGADOR'].fillna(0).astype(int)

# Change the data types to match the Oracle table
MPM_BAIXADOS_ANALITICO['NUMERO_ANTIGO'] = MPM_BAIXADOS_ANALITICO['NUMERO_ANTIGO'].astype(str)  
MPM_BAIXADOS_ANALITICO['CNJ'] = MPM_BAIXADOS_ANALITICO['CNJ'].astype(str)  
MPM_BAIXADOS_ANALITICO['CLASSE_CNJ'] = MPM_BAIXADOS_ANALITICO['CLASSE_CNJ'].astype(int) 
MPM_BAIXADOS_ANALITICO['NATUREZA'] = MPM_BAIXADOS_ANALITICO['NATUREZA'].astype(str) 
MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'] = MPM_BAIXADOS_ANALITICO['DATA_DA_BAIXA'].dt.date  
MPM_BAIXADOS_ANALITICO['MAG_ULT_CONCLUSAO'] = MPM_BAIXADOS_ANALITICO['MAG_ULT_CONCLUSAO'].astype(int) 
MPM_BAIXADOS_ANALITICO['ORGAO_JULGADOR'] = MPM_BAIXADOS_ANALITICO['ORGAO_JULGADOR'].astype(int) 
MPM_BAIXADOS_ANALITICO['RELATOR'] = MPM_BAIXADOS_ANALITICO['RELATOR'].astype(int) 
MPM_BAIXADOS_ANALITICO['OJ'] = MPM_BAIXADOS_ANALITICO['OJ'].astype(int) 
MPM_BAIXADOS_ANALITICO['ANO'] = MPM_BAIXADOS_ANALITICO['ANO'].astype(int) 
MPM_BAIXADOS_ANALITICO['MES'] = MPM_BAIXADOS_ANALITICO['MES'].astype(int) 

print(MPM_BAIXADOS_ANALITICO.dtypes)

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

    batch_size = 1000  # Define your batch size
    total_inserted = 0
    data_to_insert = []

    # Insert data into Oracle table row by row
    for index, row in MPM_BAIXADOS_ANALITICO.iterrows():
        data_to_insert.append((
            row['NUMERO_ANTIGO'], 
            row['CNJ'], 
            row['CLASSE_CNJ'], 
            row['NATUREZA'], 
            row['DATA_DA_BAIXA'], 
            row['MAG_ULT_CONCLUSAO'], 
            row['ORGAO_JULGADOR'], 
            row['RELATOR'], 
            row['OJ'],  
            row['ANO'], 
            row['MES']
        ))


        # Commit in batches
        if len(data_to_insert) >= batch_size:
            cursor.executemany(
                """
                INSERT INTO DW_DEIGE.MPM_BAIXADOS_ANALITICO 
                (NUMERO_ANTIGO, CNJ, CLASSE_CNJ, NATUREZA, DATA_DA_BAIXA, MAG_ULT_CONCLUSAO, ORGAO_JULGADOR, RELATOR, OJ, ANO, MES) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
                """, 
                data_to_insert
            )
            conn.commit()  # Commit the transaction for the batch
            logging.info(f"Total rows inserted: {total_inserted}")
            total_inserted += len(data_to_insert)
            data_to_insert.clear()  # Clear the list for the next batch

    # Insert any remaining rows after the loop
    if data_to_insert:
        cursor.executemany(
            """
            INSERT INTO DW_DEIGE.MPM_BAIXADOS_ANALITICO 
            (NUMERO_ANTIGO, CNJ, CLASSE_CNJ, NATUREZA, DATA_DA_BAIXA, MAG_ULT_CONCLUSAO, ORGAO_JULGADOR, RELATOR, OJ, ANO, MES) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
            """, 
            data_to_insert
        )
        conn.commit()  # Commit any remaining transactions
        total_inserted += len(data_to_insert)  # Increment the counter

    logging.info(f"Total rows inserted: {total_inserted}")
    logging.info("Dados inseridos com sucesso.")

except oracledb.Error as e:
    logging.error(f"Falha ao se conectar ou inserir dados no banco de dados Oracle: {e}")
finally:
    # Ensure the cursor and connection are closed properly
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
        logging.info("Conexão com o banco de dados Oracle foi fechada.")
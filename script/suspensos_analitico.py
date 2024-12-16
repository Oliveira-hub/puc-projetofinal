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

# Path to the folder containing CSV files
folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Suspensos\\ejud' 
print(f'Folder Path: {folder_path}')

# Initialize an empty DataFrame to store all the data
MPM_SUSPENSOS_ANALITICO = pd.DataFrame()

# Function to extract year and month from filename
def extract_year_month(filename):
    parts = filename.split('_')
    
    # Check if the filename has the expected format
    if len(parts) == 4 and parts[3].endswith('.csv'):
        year = parts[1]
        month = parts[2]
        return year, month
    else:
        raise ValueError("Filename format should be 'Suspensos_YYYY_MM_ANA.csv'")

for filename in os.listdir(folder_path):
    if filename.endswith('ANA.csv'):
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
                MPM_SUSPENSOS_ANALITICO = pd.concat([MPM_SUSPENSOS_ANALITICO, df], ignore_index=True)

            except Exception as e:
                print(f"Error reading {csv_file_path}: {e}")
        else:
            print(f"File not found: {csv_file_path}")

print(MPM_SUSPENSOS_ANALITICO.columns)            

MPM_SUSPENSOS_ANALITICO.columns.values[0:11] = ['CNJ','CLASSE','ORGAO_JULGADOR','RELATOR','DATA_ULT_MOV',
                                                'COD_ULT_MOV','DESCRICAO_ULT_MOV','LOCAL_FISICO_ATUAL',
                                                'LOCAL_VIRTUAL_ATUAL','ANO','MES']

# Convert 'date_column' to datetime
MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'] = MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'].str.split(' ').str[0]
MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'] = pd.to_datetime(MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'], format='%d/%m/%Y')

# Change the data types to match the Oracle table
MPM_SUSPENSOS_ANALITICO['CNJ'] = MPM_SUSPENSOS_ANALITICO['CNJ'].astype(str)
MPM_SUSPENSOS_ANALITICO['CLASSE'] = MPM_SUSPENSOS_ANALITICO['CLASSE'].astype(int)
MPM_SUSPENSOS_ANALITICO['ORGAO_JULGADOR'] = MPM_SUSPENSOS_ANALITICO['ORGAO_JULGADOR'].astype(str)
MPM_SUSPENSOS_ANALITICO['RELATOR'] = MPM_SUSPENSOS_ANALITICO['RELATOR'].astype(str)
MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'] = MPM_SUSPENSOS_ANALITICO['DATA_ULT_MOV'].dt.date
MPM_SUSPENSOS_ANALITICO['COD_ULT_MOV'] = MPM_SUSPENSOS_ANALITICO['COD_ULT_MOV'].astype(str)
MPM_SUSPENSOS_ANALITICO['DESCRICAO_ULT_MOV'] = MPM_SUSPENSOS_ANALITICO['DESCRICAO_ULT_MOV'].astype(str)
MPM_SUSPENSOS_ANALITICO['LOCAL_FISICO_ATUAL'] = MPM_SUSPENSOS_ANALITICO['LOCAL_FISICO_ATUAL'].astype(str)
MPM_SUSPENSOS_ANALITICO['LOCAL_VIRTUAL_ATUAL'] = MPM_SUSPENSOS_ANALITICO['LOCAL_VIRTUAL_ATUAL'].astype(str)
MPM_SUSPENSOS_ANALITICO['ANO'] = MPM_SUSPENSOS_ANALITICO['ANO'].astype(int)
MPM_SUSPENSOS_ANALITICO['MES'] = MPM_SUSPENSOS_ANALITICO['MES'].astype(int)

print(MPM_SUSPENSOS_ANALITICO.dtypes)

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

    logging.info("Deletando a tabela DW_DEIGE.MPM_SUSPENSOS_ANALITICO")
    cursor.execute('DELETE FROM DW_DEIGE.MPM_SUSPENSOS_ANALITICO')

    # Insert data into Oracle table row by row
    for index, row in MPM_SUSPENSOS_ANALITICO.iterrows():
        data_to_insert.append((
            row['CNJ'],
            row['CLASSE'],
            row['ORGAO_JULGADOR'],
            row['RELATOR'],
            row['DATA_ULT_MOV'],
            row['COD_ULT_MOV'],
            row['DESCRICAO_ULT_MOV'],
            row['LOCAL_FISICO_ATUAL'],
            row['LOCAL_VIRTUAL_ATUAL'],
            row['ANO'],
            row['MES']
        ))

        # Commit in batches
        if len(data_to_insert) >= batch_size:
            cursor.executemany(
                """
                INSERT INTO DW_DEIGE.MPM_SUSPENSOS_ANALITICO
                (CNJ, CLASSE, ORGAO_JULGADOR, RELATOR, DATA_ULT_MOV, COD_ULT_MOV, DESCRICAO_ULT_MOV, LOCAL_FISICO_ATUAL, LOCAL_VIRTUAL_ATUAL, ANO, MES)
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
            INSERT INTO DW_DEIGE.MPM_SUSPENSOS_ANALITICO
                (CNJ, CLASSE, ORGAO_JULGADOR, RELATOR, DATA_ULT_MOV, COD_ULT_MOV, DESCRICAO_ULT_MOV, LOCAL_FISICO_ATUAL, LOCAL_VIRTUAL_ATUAL, ANO, MES)
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
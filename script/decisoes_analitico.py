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
MPM_DECISOES_ANALITICO = pd.DataFrame()

# Function to extract year and month from filename
def extract_year_month(filename):
    # Extract year and month from the format 'CasosNovos_YYYY_MM'
    parts = filename.split('_')
    if len(parts) == 3:
        year = parts[1]
        month = parts[2].split('.')[0]  # Remove .csv extension from the month part
        return year, month
    else:
        raise ValueError("Filename format should be 'Decisoes_YYYY_MM.csv'")

# Path to the folder containing CSV files
folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Decisões\\ejud' 
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
                MPM_DECISOES_ANALITICO = pd.concat([MPM_DECISOES_ANALITICO, df], ignore_index=True)

            except Exception as e:
                print(f"Error reading {csv_file_path}: {e}")
        else:
            print(f"File not found: {csv_file_path}")

MPM_DECISOES_ANALITICO.columns.values[0:13] = ['NATUREZA','CNJ','NUM_ANT','OJ','MAGISTRADO','DATA_DECISAO','CLASSE','FASE','COMPL1','COMPL2','COMPL3','ANO','MES']

# Convert 'date_column' to datetime
MPM_DECISOES_ANALITICO['DATA_DECISAO'] = MPM_DECISOES_ANALITICO['DATA_DECISAO'].str.split(' ').str[0]
#MPM_DECISOES_ANALITICO['DATA_DECISAO'] = pd.to_datetime(MPM_DECISOES_ANALITICO['DATA_DECISAO'], format='%d/%m/%Y %H:%M:%S')
MPM_DECISOES_ANALITICO['DATA_DECISAO'] = pd.to_datetime(MPM_DECISOES_ANALITICO['DATA_DECISAO'], format='%d/%m/%Y')


#Tratando nulos
MPM_DECISOES_ANALITICO['OJ'] = MPM_DECISOES_ANALITICO['OJ'].fillna(0).astype(int)
MPM_DECISOES_ANALITICO['MAGISTRADO'] = MPM_DECISOES_ANALITICO['MAGISTRADO'].fillna(0).astype(int)
MPM_DECISOES_ANALITICO['COMPL1'] = MPM_DECISOES_ANALITICO['COMPL1'].fillna(0).astype(int)
MPM_DECISOES_ANALITICO['COMPL2'] = MPM_DECISOES_ANALITICO['COMPL2'].fillna(0).astype(int)
MPM_DECISOES_ANALITICO['COMPL3'] = MPM_DECISOES_ANALITICO['COMPL3'].fillna(0).astype(int)

# Convert to string
MPM_DECISOES_ANALITICO['NATUREZA'] = MPM_DECISOES_ANALITICO['NATUREZA'].astype(str)
MPM_DECISOES_ANALITICO['CNJ'] = MPM_DECISOES_ANALITICO['CNJ'].astype(str)
MPM_DECISOES_ANALITICO['NUM_ANT'] = MPM_DECISOES_ANALITICO['NUM_ANT'].astype(str)
# Convert to integer
MPM_DECISOES_ANALITICO['OJ'] = MPM_DECISOES_ANALITICO['OJ'].astype(int)
MPM_DECISOES_ANALITICO['MAGISTRADO'] = MPM_DECISOES_ANALITICO['MAGISTRADO'].astype(int)
MPM_DECISOES_ANALITICO['CLASSE'] = MPM_DECISOES_ANALITICO['CLASSE'].astype(int)
MPM_DECISOES_ANALITICO['FASE'] = MPM_DECISOES_ANALITICO['FASE'].astype(int)
MPM_DECISOES_ANALITICO['COMPL1'] = MPM_DECISOES_ANALITICO['COMPL1'].astype(int)
MPM_DECISOES_ANALITICO['COMPL2'] = MPM_DECISOES_ANALITICO['COMPL2'].astype(int)
MPM_DECISOES_ANALITICO['COMPL3'] = MPM_DECISOES_ANALITICO['COMPL3'].astype(int)
# Convert to date (assuming 'DATA_DECISAO' is already in a parseable date format)
MPM_DECISOES_ANALITICO['DATA_DECISAO'] = MPM_DECISOES_ANALITICO['DATA_DECISAO'].dt.date

print(MPM_DECISOES_ANALITICO.dtypes)

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

    cursor.execute("DELETE FROM DW_DEIGE.MPM_DECISOES_ANALITICO")

    # Insert data into Oracle table row by row
    for index, row in MPM_DECISOES_ANALITICO.iterrows():
        data_to_insert.append((
            row['NATUREZA'],
            row['CNJ'],
            row['NUM_ANT'],
            row['OJ'],
            row['MAGISTRADO'],
            row['DATA_DECISAO'],
            row['CLASSE'],
            row['FASE'],
            row['COMPL1'],
            row['COMPL2'],
            row['COMPL3'],
            row['ANO'],
            row['MES']
        ))


        # Commit in batches
        if len(data_to_insert) >= batch_size:
            cursor.executemany(
                """
                INSERT INTO DW_DEIGE.MPM_DECISOES_ANALITICO
                (NATUREZA, CNJ, NUM_ANT, OJ, MAGISTRADO, DATA_DECISAO, CLASSE, FASE, COMPL1, COMPL2, COMPL3, ANO, MES)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
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
            INSERT INTO DW_DEIGE.MPM_DECISOES_ANALITICO
                (NATUREZA, CNJ, NUM_ANT, OJ, MAGISTRADO, DATA_DECISAO, CLASSE, FASE, COMPL1, COMPL2, COMPL3, ANO, MES)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
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
import pandas as pd
import oracledb
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to extract year and month from filename
def extract_year_month(filename):
    try:
        parts = filename.split('_')
        year = parts[1]
        month = parts[2].split('.')[0]  # Remove .csv extension from the month part
        return year, month
    except IndexError:
        raise ValueError("Filename format should be 'CasosNovos_YYYY_MM.csv'")

# Path to the folder containing CSV files
folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Casos Novos\\ejud'
logging.info(f'Processing folder: {folder_path}')

# Configure Oracle database connection
oracle_dsn_config = {
    'host': os.getenv('ORACLE_DB_HOST'),
    'port': os.getenv('ORACLE_DB_PORT'),
    'service_name': os.getenv('ORACLE_DB_SERVICE_NAME')
}

oracle_dsn = oracledb.makedsn(
    host=oracle_dsn_config['host'],
    port=oracle_dsn_config['port'],
    service_name=oracle_dsn_config['service_name']
)

oracle_config = {
    'user': os.getenv("ORACLE_DB_USER"),
    'password': os.getenv("ORACLE_DB_PASSWORD"),
    'dsn': oracle_dsn
}

# Process each CSV file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        logging.info(f"Processing file: {filename}")
        
        # Extract year and month from filename
        try:
            year, month = extract_year_month(filename)
        except ValueError as e:
            logging.error(f"Skipping file {filename}: {e}")
            continue

        # Full path to the current CSV file
        csv_file_path = os.path.join(folder_path, filename)

        # Check if file exists
        if not os.path.exists(csv_file_path):
            logging.warning(f"File not found: {csv_file_path}")
            continue

        try:
            # Load CSV file into DataFrame
            df = pd.read_csv(csv_file_path, encoding='ISO-8859-1', delimiter=';')
            df['ANO'] = year
            df['MES'] = month

            # Rename columns to match Oracle table
            df.columns = [
                'CNJ', 'NUMERO_ANTIGO', 'PRIMEIRA_FASE', 'RELATOR', 'EX_RELATOR',
                'ORGAO_JULGADOR', 'DATA_FASE', 'FASE', 'CLASSE_CNJ', 'NATUREZA',
                'TIPO', 'ELETRONICO', 'ANO', 'MES'
            ]

            # Insert data into Oracle
            conn = oracledb.connect(**oracle_config)
            cursor = conn.cursor()

            # Delete existing records for the same year and month
            logging.info(f"Deleting records for year {year} and month {month} from the database.")
            cursor.execute(
                """
                DELETE FROM DW_DEIGE.MPM_CASOSNOVOS_ANALITICO
                WHERE ANO = :year AND MES = :month
                """, 
                {'year': year, 'month': month}
            )
            conn.commit()

            # Prepare data for insertion
            data_to_insert = df.values.tolist()
            batch_size = 1000  # Number of rows to insert in one batch
            total_inserted = 0

            # Insert data in batches
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                cursor.executemany(
                    """
                    INSERT INTO DW_DEIGE.MPM_CASOSNOVOS_ANALITICO
                    (CNJ, NUMERO_ANTIGO, PRIMEIRA_FASE, RELATOR, EX_RELATOR, ORGAO_JULGADOR, 
                     DATA_FASE, FASE, CLASSE_CNJ, NATUREZA, TIPO, ELETRONICO, ANO, MES) 
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
                    """, 
                    batch
                )
                conn.commit()
                total_inserted += len(batch)
            
            logging.info(f"Inserted {total_inserted} rows from {filename} into the database.")

        except Exception as e:
            logging.error(f"Failed to process file {filename}: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
                logging.info("Database connection closed.")






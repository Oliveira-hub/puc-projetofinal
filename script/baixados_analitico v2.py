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
        raise ValueError("Filename format should be 'Baixados_YYYY_MM.csv'")

# Function to process a single CSV file
def process_csv_file(csv_file_path, year, month):
    try:
        # Load CSV file into DataFrame
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1', delimiter=';')

        # Use only the first 9 columns
        df = df.iloc[:, :9]

        # Add 'ANO' and 'MES' columns
        df['ANO'] = year
        df['MES'] = month

        # Rename columns to match Oracle table
        df.columns = [
            'NUMERO_ANTIGO', 'CNJ', 'CLASSE_CNJ', 'NATUREZA', 'DATA_DA_BAIXA',
            'MAG_ULT_CONCLUSAO', 'ORGAO_JULGADOR', 'RELATOR', 'OJ', 'ANO', 'MES'
        ]

        # Convert 'DATA_DA_BAIXA' to datetime
        df['DATA_DA_BAIXA'] = df['DATA_DA_BAIXA'].str.split(' ').str[0]
        df['DATA_DA_BAIXA'] = pd.to_datetime(df['DATA_DA_BAIXA'], format='%d/%m/%Y', errors='coerce')

        # Handle missing values for integer columns
        integer_columns = ['CLASSE_CNJ', 'MAG_ULT_CONCLUSAO', 'ORGAO_JULGADOR', 'RELATOR', 'OJ']
        for col in integer_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert non-numeric values to NaN
            df[col] = df[col].fillna(0).astype(int)           # Replace NaN with 0 and convert to int

        # Change data types to match the Oracle table
        df['NUMERO_ANTIGO'] = df['NUMERO_ANTIGO'].astype(str)
        df['CNJ'] = df['CNJ'].astype(str)
        df['NATUREZA'] = df['NATUREZA'].astype(str)
        df['DATA_DA_BAIXA'] = df['DATA_DA_BAIXA'].dt.date
        df['ANO'] = df['ANO'].astype(int)
        df['MES'] = df['MES'].astype(int)

        logging.info(f"Processed file {csv_file_path} successfully.")
        return df

    except Exception as e:
        logging.error(f"Error processing file {csv_file_path}: {e}")
        return None

# Function to insert DataFrame into Oracle database
def insert_into_database(df, year, month, oracle_config):
    try:
        # Connect to Oracle database
        conn = oracledb.connect(**oracle_config)
        cursor = conn.cursor()

        # Delete existing records for the same year and month
        logging.info(f"Deleting records for year {year} and month {month} from the database.")
        cursor.execute(
            """
            DELETE FROM DW_DEIGE.MPM_BAIXADOS_ANALITICO
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
                INSERT INTO DW_DEIGE.MPM_BAIXADOS_ANALITICO 
                (NUMERO_ANTIGO, CNJ, CLASSE_CNJ, NATUREZA, DATA_DA_BAIXA, MAG_ULT_CONCLUSAO, ORGAO_JULGADOR, RELATOR, OJ, ANO, MES) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
                """,
                batch
            )
            conn.commit()
            total_inserted += len(batch)

        logging.info(f"Inserted {total_inserted} rows into the database.")

    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")

# Main script

folder_path = 'E:\\2_Instancia\\estatisticas_mpm\\Baixados\\ejud'
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
        
        try:
            year, month = extract_year_month(filename)
        except ValueError as e:
            logging.error(f"Skipping file {filename}: {e}")
            continue

        csv_file_path = os.path.join(folder_path, filename)

        if not os.path.exists(csv_file_path):
            logging.warning(f"File not found: {csv_file_path}")
            continue

        # Process the CSV file
        df = process_csv_file(csv_file_path, year, month)

        if df is not None:
            # Insert processed data into Oracle
            insert_into_database(df, year, month, oracle_config)

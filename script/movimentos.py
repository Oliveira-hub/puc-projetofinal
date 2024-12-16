import pandas as pd
import oracledb
import os
from dotenv import load_dotenv
import logging
from datetime import datetime


# File path to save the CSV
csv_file_path = "E:\\2_Instancia\\PROCEDURES PARA ANALISE\\script\\extracted\\movimento.csv"

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

### EXTRAÇÃO

# Insert each row into the Oracle table
conn = None

# Initialize an empty DataFrame to store all batches
all_data = pd.DataFrame()

# Function to write the header to the CSV if it doesn't exist
def create_csv_with_header_if_not_exists(cursor, csv_file_path):
    if not os.path.exists(csv_file_path):
        logging.info(f"File {csv_file_path} does not exist. Creating the file with headers.")
        
        # Fetch column names for the DataFrame
        cursor.execute("SELECT * FROM ejud.movimento@tj01 WHERE 1=0")  # Returns just the column names
        columns = [col[0] for col in cursor.description]

        # Write the column headers to the CSV file
        pd.DataFrame(columns=columns).to_csv(csv_file_path, index=False, header=True)
        logging.info(f"File {csv_file_path} created with headers: {columns}")

# Helper function to read the last extracted DTHRMOV from the CSV file
def read_last_extracted_time_from_csv(filename=csv_file_path):
    # If the file doesn't exist, return the default datetime
    if not os.path.exists(filename):
        return datetime(2024, 8, 31, 23, 59, 59)

    try:
        # Read the CSV file
        df = pd.read_csv(filename)
        # Check if 'DTHRMOV' column exists and get the maximum value
        if 'DTHRMOV' in df.columns:
            # Convert to datetime and return the max value
            return pd.to_datetime(df['DTHRMOV']).max()
    except Exception as e:
        logging.error(f"Error reading last extracted time from CSV: {e}")
    
    # Return the default datetime if there's an issue
    return datetime(2024, 8, 31, 23, 59, 59)

# Ensure the directory for the CSV file exists
directory = os.path.dirname(csv_file_path)

# Initialize batch size and offset
batch_size = 10000
offset = 0

# Read the last extracted DTHRMOV time
try:
    # Read the last extracted DTHRMOV time from the CSV file
    last_extracted_time = read_last_extracted_time_from_csv()

    # If the last extracted time is None (file does not exist), set it to the earliest possible date
    if last_extracted_time is None:
        last_extracted_time = datetime(2024, 8, 31, 23, 59, 59)
except Exception as e:
    logging.error(f"Error reading last extracted time: {e}")
    # Fallback to the earliest possible date in case of an error

# SQL query using OFFSET and FETCH for batch processing
sql = """
    SELECT * FROM ejud.movimento@tj01
    WHERE DTHRMOV > TO_TIMESTAMP(:last_extracted_movement_time, 'YYYY-MM-DD HH24:MI:SS')
    and DTHRMOV <= sysdate
    OFFSET :offset ROWS FETCH NEXT :batch_size ROWS ONLY
"""

try:
    logging.info("Conectando-se ao banco de dados Oracle.")
    conn = oracledb.connect(**oracle_config)
    cursor = conn.cursor()

    # Ensure the directory for the CSV file exists
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    # Ensure the CSV file is created with headers if it doesn't exist
    create_csv_with_header_if_not_exists(cursor, csv_file_path)

    # Log the start of the extraction process
    logging.info("Data extraction started.")

    while True:

        # Prepare the parameters for execution
        params = {
            'last_extracted_movement_time': last_extracted_time.strftime("%Y-%m-%d %H:%M:%S"),
            'offset': offset,
            'batch_size': batch_size
        }

        # Execute the query with parameters
        cursor.execute(sql, params)
        
        # Fetch the rows
        rows = cursor.fetchall()

        # Log the total number of rows fetched
        total_rows_fetched = len(rows)
        logging.info(f"Total rows fetched: {total_rows_fetched}")
        
        # If no more rows, exit the loop
        if not rows:
            logging.info("No more rows to fetch. Extraction completed.")
            break
        
        # Fetch column names for the DataFrame (already fetched above, this is optional)
        columns = [col[0] for col in cursor.description]
        
        # Convert the rows to a pandas DataFrame for the current batch
        batch_df = pd.DataFrame(rows, columns=columns)
        
        # Append the current batch to the CSV file
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as f:
            batch_df.to_csv(f, index=False, header=False)   
            # Log the total number of rows appended to CSV
            logging.info(f"Appended {total_rows_fetched} rows to {csv_file_path}.")    

        # Increment offset by batch size to get the next batch
        offset += batch_size


except oracledb.Error as e:
    logging.error(f"Falha ao se conectar ou inserir dados no banco de dados Oracle: {e}")
finally:
    # Ensure the cursor and connection are closed properly
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logging.info("Conexão com o banco de dados Oracle foi fechada.")
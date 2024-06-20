import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from datetime import datetime

load_dotenv()

def connect_db():
    """Connect to DuckDB; Create DB if not exists."""
    return duckdb.connect(database='duckdb.db', read_only=False)

def create_table(con):
    """Create table if not exists."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS files_history (
            file_name VARCHAR,
            process_time TIMESTAMP
        )
    """)

def register_file(con, file_name):
    """Registra um novo arquivo no banco de dados com o horário atual."""
    con.execute("""
        INSERT INTO files_history (file_name, process_time)
        VALUES (?, ?)
    """, (file_name, datetime.now()))

def get_processed_files(con):
    """Returns a set with the file_names already processed.."""
    return set(row[0] for row in con.execute("SELECT file_name FROM files_history").fetchall())

def download_gdrive_files(url_dir, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    gdown.download_folder(url_dir, output=local_dir, quiet=False, use_cookies=False)


def list_files_and_filetypes(local_dir):
    """ List files ad identifies csv, json, or parquet"""
    files_and_filetypes = []
    all_files = os.listdir(local_dir)

    for file in all_files:
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".parquet"):
            path = os.path.join(local_dir, file)
            filetype = file.split(".")[-1]
            files_and_filetypes.append((path, filetype))

    return files_and_filetypes

def read_file(path, filetype):
    """Reads the file depending on filetype"""

    if filetype == 'csv':
        return duckdb.read_csv(path)
    elif filetype == 'json':
        return pd.read_json(path)
    elif filetype == 'parquet':
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Filetype not supported: {filetype}")
    

# converts a duckdb dataframe to pandas dataframe
def transform(df: DuckDBPyRelation) -> pd.DataFrame:
    new_df = duckdb.sql("SELECT *, quantity * price AS total_sales FROM df").df()
    return new_df

def save_to_postgres(df_pandas, table_name):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    df_pandas.to_sql(table_name, con=engine, if_exists='append', index=False)


def pipeline():
    url_dir = os.getenv("GDRIVE_URL")
    local_dir = './pasta_gdown'

    #download_gdrive_files(url_dir, local_dir)
    files_list = list_files_and_filetypes(local_dir)

    con = connect_db()
    create_table(con)
    processed_files = get_processed_files(con)
    
    logs = []

    for file_path, filetype in files_list:
        file_name = os.path.basename(file_path)

        if file_name not in processed_files:
            df_duckdb = read_file(file_path, filetype)
            new_df_pandas = transform(df_duckdb)
            save_to_postgres(new_df_pandas,'sales_calculated')

            register_file(con, file_name)
            print(f"File {file_name} processed and loaded to database.")
            logs.append(f"File {file_name} processed and loaded to database.")
        
        else:
            print(f"File {file_name} already processed previously.")
            logs.append(f"File {file_name} already processed previously.")
    
    return logs

if __name__ == "__main__":
    pipeline()
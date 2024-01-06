from sqlalchemy import create_engine, MetaData, Table
import os
import pandas as pd

def get_db_data(schema_name, table_name):
    # Data
    dbname = os.environ.get("DATA_DB_NAME")
    user = os.environ.get("DATA_DB_USER")
    password = os.environ.get("DATA_DB_PASSWORD")
    host = os.environ.get("DATA_DB_HOST")
    # Construct the database URL
    database_url = f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
    # Create an SQLAlchemy engine and connect
    engine = create_engine(database_url)
    conn = engine.connect()
    # Reflect the table structure using MetaData
    table = Table(table_name, MetaData(), autoload_with=engine, schema=schema_name)
    # Select all data from the table and execute the query
    query = table.select()
    result = conn.execute(query)
    rows = result.fetchall()
    # Results to df
    df = pd.DataFrame(rows, columns=result.keys())
    # Close the connection
    conn.close()
    return df
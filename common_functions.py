from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv


def execute_mysql_query(query: str) -> pd.DataFrame:
    # Define the host, username, password, and database
    host = os.environ.get('HOST')
    username = os.environ.get('USERNAME')
    password = os.environ.get('PASSWORD')
    database = os.environ.get('DATABASE')

    # Create a database connection string
    ssl_args = {'ssl_ca': '../../cert/cacert.pem'}
    db_connection_str = f'mysql+pymysql://{username}:{password}@{host}/{database}?ssl=true'

    # Create a database connection
    db_connection = create_engine(db_connection_str, connect_args=ssl_args)

    # Execute a SQL query and retrieve the results as a pandas DataFrame
    df = pd.read_sql(query, con=db_connection)

    # Close the database connection
    db_connection.dispose()

    return df

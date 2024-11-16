
from azure.identity import AzureCliCredential
from enum import Enum
import struct
from itertools import chain, repeat
import pyodbc
import pandas as pd
import os, datetime, re
import time
import warnings

# Ignore the specific UserWarning from pandas
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

class ConnectionType(Enum):
    SQL_SERVER = "sql_server"
    FABRIC_LAKEHOUSE = "fabric_lakehouse"

class SqlStorage:
    def __init__(self, connection_type=None, 
                 host=None, 
                 port=None, 
                 database=None, 
                 username=None, 
                 password=None):
        
        self.connection_type = connection_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.conn = None  
        
        if self.host is None or self.database is None:
            raise ValueError(f"Host and database must be provided for database connection {e}")

        try: 
            if connection_type == ConnectionType.FABRIC_LAKEHOUSE:
                # Initialize for Azure Fabric Lakehouse with token-based authentication
                self.credential = AzureCliCredential()
                token_object = self.credential.get_token("https://database.windows.net/.default")
                token_as_bytes = bytes(token_object.token, "UTF-8")
                encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
                token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
                self.attrs_before = {1256: token_bytes}  # SQL_COPT_SS_ACCESS_TOKEN attribute
                
                self.connection_string = (
                    f"Driver={{ODBC Driver 18 for SQL Server}};"
                    f"Server={self.host};"
                    f"Database={self.database};"
                    "Encrypt=Yes;TrustServerCertificate=No;"
                )
                
            elif connection_type == ConnectionType.SQL_SERVER:
                # Initialize for a standard SQL Server instance with Windows Authentication
                
                if self.username and self.password:
                    self.connection_string = (
                        f"Driver={{ODBC Driver 18 for SQL Server}};"
                        f"Server={self.host};"
                        f"Database={self.database};"
                        f"Uid={self.username};Pwd={self.password};"
                        "Encrypt=no;"
                    )
                else:
                    # Use Trusted Connection (Windows Authentication)
                    self.connection_string = (
                        f"Driver={{ODBC Driver 18 for SQL Server}};"
                        f"Server={self.host};"
                        f"Database={self.database};"
                        "Trusted_Connection=yes;Encrypt=no;"
                    )
                self.attrs_before = None  # No token-based attributes required
        except Exception as e:
            msg = f"Error during SQL setup {connection_type}: {str(e)}"
            print(msg)
            raise Exception(msg)
            
        self.connect()

    def connect(self):
        try:
            if self.connection_type == ConnectionType.FABRIC_LAKEHOUSE:
                self.conn = pyodbc.connect(
                    self.connection_string,
                    attrs_before=self.attrs_before
                )
                print("Connection successful to Lakehouse")

            elif self.connection_type == ConnectionType.SQL_SERVER:
                self.conn = pyodbc.connect(self.connection_string)
                print("Connection successful to MSSQL")
                
        except Exception as e:
            msg = f"Error occurred during connection: {str(e)}" 
            print(msg)
            raise Exception(msg)

    def sanitize_table_name(self, table_name: str) -> str:
        """
        Replaces invalid SQL Server characters in a table name with "_".
        """
        sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
        return sanitized_name

    def map_dtype_to_sql(self, dtype):
        # Map pandas DataFrame dtypes to SQL Server data types
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BIT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        else:
            # Default to NVARCHAR with a reasonable maximum length for non-numeric types
            return "NVARCHAR(255)"
        
    def execute_query(self, query):
        """Execute a SQL query and return the result as a pandas DataFrame."""
        try:
            # Ensure the connection is active
            if self.conn is None:
                self.connect()

            # Execute the query and return the result as a DataFrame
            df = pd.read_sql(query, self.conn)
            return df

        except Exception as e:
            msg = f"Error executing query: {e}"
            print(msg)
            raise Exception(msg)

    def upload_dataframe_to_table(self, df, table_name):
        
        if self.conn is None:
            self.connect()
        
        table_name = self.sanitize_table_name(table_name)
        
        # Define the table structure based on dataframe with appropriate column types
        column_definitions = ", ".join([f"{col} {self.map_dtype_to_sql(dtype)}" for col, dtype in df.dtypes.items()])

        # Generate SQL to drop and recreate the table
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        create_table_query = f"CREATE TABLE {table_name} ({column_definitions});"

        try:
            # Drop table and recreate it
            with self.conn.cursor() as cursor:
                cursor.execute(drop_table_query)
                cursor.execute(create_table_query)

            # Insert data into the table
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in df.columns])})"
            with self.conn.cursor() as cursor:
                cursor.executemany(insert_query, df.values.tolist())
            self.conn.commit()
            print(f"Data successfully uploaded to table '{table_name}'")
        
        except Exception as e:
            msg = f"Error occurred during CSV upload: {str(e)}"
            print(msg)
            raise Exception(msg)

    def upload_csv_to_table(self, csv_path, table_name):
        # Load CSV into DataFrame and call `upload_dataframe_to_table`
        df = pd.read_csv(csv_path)
        self.upload_dataframe_to_table(df, table_name)

    def close(self):
        if self.conn:
            self.conn.close()
            print('Connection closed')

    def generate_sequential_table_name(self, prefix):
        today_str = datetime.datetime.now().strftime("%d_%m_%Y")
        pattern = rf"{prefix}_{today_str}_(\d{{3}})"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT name FROM sys.tables WHERE name LIKE '{prefix}_{today_str}%'")
                table_names = [row[0] for row in cursor.fetchall()]

            max_sequence = 0
            for table_name in table_names:
                match = re.search(pattern, table_name)
                if match:
                    sequence_num = int(match.group(1))
                    max_sequence = max(max_sequence, sequence_num)

            next_sequence = max_sequence + 1
            next_sequence_str = f"{next_sequence:03}"
            new_table_name = f"{prefix}_{today_str}_{next_sequence_str}"
            return new_table_name
        
        except Exception as e:
            print("Error occurred while generating sequential table name:", str(e))
            return None

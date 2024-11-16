try:
    import pyodbc
    import pandas as pd
    import os
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please ensure you have installed the required packages:")
    print("pip install pyodbc pandas")
    exit(1)

class FabricLakehouseQueryExecutor:
    def __init__(self, analytics_endpoint, database):
        self.analytics_endpoint = analytics_endpoint
        self.database = database
        self.conn = None

    def connect(self):
        """Establish connection to Fabric Lakehouse"""
        try:
            conn_str = (
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={self.analytics_endpoint};"
                f"Database={self.database};"
                "Authentication=ActiveDirectoryInteractive;"
            )
            self.conn = pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            print(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query):
        """Execute a SQL query"""
        if not self.conn:
            raise ValueError("Connection not established. Call connect() first.")
        return pd.read_sql(query, self.conn)

    def save_as_csv(self, dataframe, local_path, filename):
        """Save a DataFrame as a CSV file"""
        full_path = os.path.join(local_path, filename)
        dataframe.to_csv(full_path, index=False)
        return full_path

    def disconnect(self):
        """Close the connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def query_and_save(self, query, local_path, filename):
        """Execute a query and save results as CSV"""
        try:
            self.connect()
            result = self.execute_query(query)
            saved_path = self.save_as_csv(result, local_path, filename)
            print(f"Query results saved to {saved_path}")
        except Exception as e:
            print(f"Error during query execution or saving: {e}")
        finally:
            self.disconnect()


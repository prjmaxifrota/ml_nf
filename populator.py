
import pandas as pd
import os, sys 
from services.sql_storage import ConnectionType, SqlStorage
from services.file_services import FileServices

import time 

class Populator:
    def __init__(self):
        
        self.file_service = FileServices()
        self.file_service.blob_connection_string = os.getenv('BLOB_CONN_STR')
        self.file_service.blob_container_name = os.getenv('BLOB_CONTAINER')
        self.blob_dir = os.getenv('BLOB_DIR')
        self.blob_dir_sql = os.getenv('BLOB_DIR_SQL')
    
    def initialize(self, df_params=None):
        
        if df_params:   
            self.refresh_source = df_params['refresh_source']    
            self.csv_remote_name = df_params['csv_remote_name']
            self.sql_query = df_params['sql_query']
            self.save_results_to_db = df_params['save_results_to_db']
            self.experiment_name = df_params['experiment_name']
            self.gen_path = df_params['gen_path']
            
            if self.save_results_to_db is None: 
                self.save_results_to_db = False
                
            if self.refresh_source == 'azure_csv':
                self.log_status(f'\nGenerating dataframe from Azure Blob CSV file...')
                start_time = time.time()
                self.df_analysis = self.read_df_from_azure(self.blob_dir + self.csv_remote_name)
                self.log_status(f'Generating dataframe from Azure Blob CSV file completed in {time.time() - start_time:.2f} seconds.\n')
            elif self.refresh_source == 'sql':
                if self.sql_query is None or self.sql_query == '': 
                    raise Exception('Cannot refresh dataframe because SQL query was not provided.')   
                
                start_time = time.time()
                self.log_status(f'\nGenerating dataframe from SQL query...')
                self.df_analysis = self.read_df_from_sql(df_params)
                self.log_status(f'Generating dataframe from SQL Query completed in {time.time() - start_time:.2f} seconds.\n')
                
                try:
                    start_time = time.time()
                    self.log_status(f'\nWriting dataframe generated from SQL Query to CSV into Azure Blob...')
                    self.file_service.write_azure_blob_dataframe(self.df_analysis, self.blob_dir + self.csv_remote_name)
                    self.log_status(f'Writing dataframe generated from SQL Query to CSV into Azure Blob completed in {time.time() - start_time:.2f} seconds.\n')    
                except Exception as e:
                    self.log_status(f'Error writing dataframe as CSV to Azure: {e}')    
                    
            else:
                self.df_analysis = None
        
        if self.df_analysis is None:
            raise Exception('Cannot process with empty dataframe.')
    
    def log_status(self, msg, raise_exception=False):
        print(msg)
        
        if raise_exception: 
            raise Exception(msg)    

    def read_df_from_azure(self, remote_csv=None):
        df = None
        if remote_csv is not None:
            try:
                df = self.file_service.read_azure_blob_dataframe(remote_csv)
            except Exception as e:  
                self.log_status(f'Error reading CSV from Azure: {e}')
        self.df_analysis = df
        return self.df_analysis
    
    def read_sql_from_azure(self, remote_sql=None):
        sql = None
        if remote_sql is not None:
            try:
                sql = self.file_service.read_azure_blob_text(remote_sql)
            except Exception as e:  
                self.log_status(f'Error reading SQL file from Azure: {e}')
        return sql
    
    def read_df_from_sql(self, params=None):
        
        host = os.getenv('RESOURCE_SQL_HOSTNAME')
        port = os.getenv('RESOURCE_SQL_PORT', 1433)
        user_name = os.getenv('RESOURCE_SQL_USERNAME')
        user_pwd = os.getenv('RESOURCE_SQL_PASSWORD')
        database = os.getenv('RESOURCE_SQL_DATABASE')
        sample_size = str(params['sample_size'])
        sql_file = params['sql_query']
        
        sql_query = None
        try:
            start_time = time.time()
            self.log_status(f'\nReading SQL text file from Azure...')
            sql_query = self.read_sql_from_azure(self.blob_dir_sql + sql_file)
            self.log_status(f'Reading SQL text file from Azure completed in {time.time() - start_time:.2f} seconds.\n')
        except Exception as e:
            self.log_status(f"\nError reading SQL file from Azure: {e}, exit.", True)
        
        executor = None
        error = False
        max_attempts = 3
        exception = None
        for i in range(max_attempts):
            try:
                executor = SqlStorage(
                    connection_type=ConnectionType.SQL_SERVER, 
                    host=host, 
                    port=port, 
                    database=database, 
                    username=user_name, 
                    password=user_pwd)
                error = False
                break
            except Exception as e:
                error = True
                exception = e
                self.log_status(f"\nError connecting to SQL: {e}, retry attempt {i} of {max_attempts}...")
                time.sleep(3)
        
        if error:
            self.log_status(f"\nError connecting to SQL: {exception}, exit.", True)
        
        if not error:
            sql_query = sql_query.replace('{sample_size}', sample_size)
            try:
                df = executor.execute_query(sql_query)
                self.df_analysis = df
            except Exception as e:
                self.df_analysis = None
                self.log_status(f'Error executing SQL Query {e}', True) 
        else:
            self.df_analysis = None
        
        return self.df_analysis

    
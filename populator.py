
import pandas as pd
import os, sys 
from services.sql_storage import ConnectionType, SqlStorage
from services.file_services import FileServices

import time 

class FeatureImpactAnalyzerConsumer:
    def __init__(self):
        
        self.analyzer = None
        
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
                self.log_status(f'\nGenerating dataframe from Lakehouse SQL query...')
                self.df_analysis = self.read_df_from_sql(df_params)
                self.log_status(f'Generating dataframe from Lakehouse SQL Query completed in {time.time() - start_time:.2f} seconds.\n')
                
                try:
                    start_time = time.time()
                    self.log_status(f'\nWriting dataframe generated from Lakehouse SQL Query to CSV into Azure Blob...')
                    self.file_service.write_azure_blob_dataframe(self.df_analysis, self.blob_dir + self.csv_remote_name)
                    self.log_status(f'Writing dataframe generated from Lakehouse SQL Query to CSV into Azure Blob completed in {time.time() - start_time:.2f} seconds.\n')    
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
        
        analytics_endpoint = os.getenv('ANALYTICS_ENDPOINT')
        database = os.getenv('ANALYTICS_DATABASE')
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
                    connection_type=ConnectionType.FABRIC_LAKEHOUSE, 
                    host=analytics_endpoint, 
                    port=None, 
                    database=database, 
                    username=None, 
                    password=None)
                error = False
                break
            except Exception as e:
                error = True
                exception = e
                self.log_status(f"\nError connecting to the lakehouse: {e}, retry attempt {i} of {max_attempts}...")
                time.sleep(3)
        
        if error:
            self.log_status(f"\nError connecting to the lakehouse: {exception}, exit.", True)
        
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

    def run_analysis(self, params):
        
        if self.df_analysis is None or self.analyzer is None:
            raise Exception('Cannot process with uninitialized Analizer due to empty dataframe.')
        
        start_time_analysis = time.time()
        self.log_status('\nStarting analysis...\n')
        
        experiment_name = params['experiment_name']
        gen_path = params['gen_path']
        id_col = params['id_col']
        numerical_column = params['numerical_column']
        impact_cols = params['impact_cols']
        replace_col = params['replace_col']
        categorical_columns = params['categorical_columns']
        
        self.df_analysis = self.analyzer.df
        df_analysis = self.df_analysis 
        
        # Step 1: Connect to data storage and upload CSV
        
        sql_results_host = os.getenv('RESULTS_SQL_HOSTNAME', 'localhost')
        sql_results_port = os.getenv('RESULTS_SQL_PORT', 1433)
        sql_results_username = os.getenv('RESULTS_SQL_USERNAME')
        sql_results_password = os.getenv('RESULTS_SQL_PASSWORD')
        sql_results_database = os.getenv('RESULTS_SQL_DATABASE')
        
        self.analyzer.connectDataStorage(connection_type=ConnectionType.SQL_SERVER,
                                         host=sql_results_host, 
                                         port=sql_results_port, 
                                         database=sql_results_database, 
                                         username=sql_results_username, 
                                         password=sql_results_password, 
                                         save_results=self.save_results_to_db)
        
        self.analyzer.upload_dataframe_to_table(df=df_analysis, 
                                                table_name=experiment_name + '_clustered_trn', 
                                                save_results=self.save_results_to_db)
        
        start_time = time.time()
        self.log_status('Starting indexing...')
        self.analyzer.create_index_for_impactful_columns(df_analysis, categorical_columns)
        self.log_status(f'\nIndexing completed in {time.time() - start_time:.2f} seconds.\n')
        
        """
        # Run the ML workflow
        ml_interpreted_df = self.analyzer.run_ml_workflow_on_impactful_columns(impact_cols, self.analyzer.df, 
                                        numerical_column, gen_path, id_col='idtransacao')
        path_ml_csv = os.path.join(gen_path, '000-ml-impactfull_columns.csv')
        ml_interpreted_df.to_csv(path_ml_csv, index=True)
        """

        # Step 2: Create slides and calculate correlation
        slides = self.analyzer.create_intro_slides(gen_path, cover_image=params['cover_image'], slides=None)
        path_eta_csv = os.path.join(gen_path, '001-correlation_ratio_eta_equared.csv')
        imag_path = os.path.join(gen_path, '001-correlation_ratio_eta_equared.png')
        
        start_time = time.time()
        self.log_status('\nStarting correlation calculation...')
        sorted_eta_squared = self.analyzer.correlation_ratio_and_plot(
            params['categorical_columns'], numerical_column, image_path=imag_path, csv_path=path_eta_csv
        )
        self.analyzer.upload_csv_to_table(csv_path=path_eta_csv, 
                                          table_name=experiment_name + '_impacto_rank', 
                                          save_results=self.save_results_to_db)
        self.log_status(f'Correlation calculation completed in {time.time() - start_time:.2f} seconds.\n')

        slides = self.analyzer.add_impact_scores_slides(imag_path=imag_path, num_col=numerical_column, slides=slides)
        df_eta = pd.read_csv(path_eta_csv)
        slides = self.analyzer.add_df_slide(title=f'Atributos mais impactantes para {numerical_column}', df=df_eta, slides=slides)

        # Step 3: Get top contributing categorical values
        start_time = time.time()
        self.log_status('Starting top contributing category values calculation...')
        sorted_eta_squared = self.analyzer.get_top_contributing_category_values_many(
            impact_cols, sorted_eta_squared, numerical_column, additional_cols=[], top_n_values=params['top_n'], csv_path=None
        )
        self.log_status(f'Top contributing category values calculation completed in {time.time() - start_time:.2f} seconds.\n')

        # Step 4: Flatten and upload contributing category values
        start_time = time.time()
        self.log_status('Starting flattening top contributing category values ...')
        cat_values_df_path = os.path.join(gen_path, '002-top_contributing_category_values.csv')
        df_top_categories_value_df = self.analyzer.flatten_contributing_category_values_to_dataframe(
            data_tuples=sorted_eta_squared[1], cols_to_extract_from_dict=['impact', 'count', 'good_or_bad'], csv_path=cat_values_df_path
        )
        self.analyzer.upload_csv_to_table(csv_path=cat_values_df_path, 
                                          table_name=experiment_name + '_atributo_impacto', 
                                          save_results=self.save_results_to_db)
        self.log_status(f'Flattening top contributing category values completed in {time.time() - start_time:.2f} seconds.\n')
        slides = self.analyzer.add_df_slide(title=f'Entidades mais impactantes para {numerical_column}', df=df_top_categories_value_df, slides=slides)

        # Step 5: Get top contributing records and recommendations
        start_time = time.time()
        self.log_status('Starting top contributing records calculation...')
        contrib_impact_records_path = os.path.join(gen_path, '003-top_contributing_records_eta.csv')
        top_contributing_records_eta = self.analyzer.get_top_contributing_records_based_on_eta_many(
            id_col=id_col, categorical_cols=impact_cols, sorted_eta_squared=sorted_eta_squared,
            numerical_col=numerical_column, top_n=50, top_n_records=params['top_n_records'], csv_path=contrib_impact_records_path
        )
        self.analyzer.upload_csv_to_table(csv_path=contrib_impact_records_path, 
                                          table_name=experiment_name + '_registro_impacto', 
                                          save_results=self.save_results_to_db)
        self.log_status(f'Top contributing records calculation completed in {time.time() - start_time:.2f} seconds.\n')
        slides = self.analyzer.add_df_slide(title=f'Registros mais impactantes para {numerical_column}', df=top_contributing_records_eta, slides=slides)

        # Step 6: Get recommended categories using decision trees
        start_time = time.time()
        self.log_status('Starting recommended categories calculation...')
        recommended_category_values_dtrees_path = os.path.join(gen_path, '004-recommended_categories_dtrees.csv')
        recommended_categories_dtrees = self.analyzer.get_top_recommended_category_values_many_dtrees(
            df=df_analysis, filtered_df=top_contributing_records_eta,
            impactant_category_values_dict=sorted_eta_squared, categorical_cols=impact_cols,
            categorical_cols_to_replace=[replace_col], numerical_col=numerical_column,
            percent_less_num_col=params['percent_less_num_col'], percent_less_add_col=params['percent_less_add_col'],
            csv_path=recommended_category_values_dtrees_path
        )
        recommended_category_values_dtrees_df = pd.read_csv(recommended_category_values_dtrees_path)
        self.analyzer.upload_dataframe_to_table(df=recommended_category_values_dtrees_df, 
                                                table_name=experiment_name + '_atributo_recomendacao', 
                                                save_results=self.save_results_to_db)
        self.log_status(f'Recommended categories calculation completed in {time.time() - start_time:.2f} seconds.\n')
        slides = self.analyzer.add_df_slide(title=f'Substituições recomendadas {replace_col}', df=recommended_category_values_dtrees_df, slides=slides)

        # Step 7: Get recommended records using decision trees
        start_time = time.time()
        self.log_status('Starting recommended records calculation...')
        recomended_records_dtree_path = os.path.join(gen_path, '005-recommended_records_dtrees.csv')
        recommended_records_dtrees = self.analyzer.get_top_recommended_record_values_many_dtrees(
            id_col=id_col, recommended_combinations=recommended_categories_dtrees, df=df_analysis,
            categorical_cols=impact_cols, categorical_cols_to_replace=[replace_col], numerical_col=numerical_column,
            csv_path=recomended_records_dtree_path
        )
        recommended_records_dtrees_df = pd.read_csv(recomended_records_dtree_path)
        self.analyzer.upload_dataframe_to_table(df=recommended_records_dtrees, 
                                                table_name=experiment_name + '_registro_recomendacao', 
                                                save_results=self.save_results_to_db)
        self.log_status(f'Recommended records calculation completed in {time.time() - start_time:.2f} seconds.\n')
        slides = self.analyzer.add_df_slide(title=f'Registros com as substituições recomendadas {replace_col}', df=recommended_records_dtrees_df, slides=slides)

        self.log_status(f'Analysis completed in {time.time() - start_time_analysis:.2f} seconds.\n')

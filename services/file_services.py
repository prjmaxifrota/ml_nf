import pandas as pd
from azure.storage.blob import BlobServiceClient 
from io import StringIO 
import pickle
from joblib import dump, load
import yaml
import importlib
import os 

class FileServices:

    def __init__(self):
        self.name = "File Services"

        self._remote_storage_type = None
        self._blob_connection_string = None
        self._blob_container_name = None
        self._blob_name = None
        self._blob_service_client = None
        self._container_client = None    

    def get_name(self):
        return self.name

    @property
    def remote_storage_type(self):
        return self._remote_storage_type

    @remote_storage_type.setter
    def remote_storage_type(self, value):
        self._remote_storage_type = value

    @property
    def blob_connection_string(self):
        return self._blob_connection_string

    @blob_connection_string.setter
    def blob_connection_string(self, value):
        self._blob_connection_string = value

    @property
    def blob_container_name(self):
        return self._blob_container_name

    @blob_container_name.setter
    def blob_container_name(self, value):
        self._blob_container_name = value
        
    @property
    def blob_service_client(self):
        # Create the BlobServiceClient object
        if self._blob_service_client is None:
            self._blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
        
        return self._blob_service_client

    @property
    def container_client(self):
        if self._container_client is None:
            self._container_client = self.blob_service_client.get_container_client(self.blob_container_name)
        
        return self._container_client 
    
    def pipeline_find_first(self, directory_path):
        try:
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    # Check if the file contains 'pipeline' in its name and has a '.yaml' extension
                    if 'pipeline' in file and file.endswith('.yaml'):
                        return file
            return None
        except Exception as e:
            raise e

    def create_directory_if_not_exists(self, directory_path) -> bool:
        try: 
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
                # Indicates directory created
                return True
            else:
                # Indicates directory already exists
                return False
        except Exception as e:
            raise e
        
    def read_from_file(self, filename): 
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                data = file.read()
            return data
        except:
            return ''
        
    def write_to_file(self, filename, content, mode='w'):
        with open(filename, mode, encoding='utf-8') as file:
            file.write(content)
    
    def read_yaml_config(self, path: str = '__config__.yaml') -> any:
        
        with open(path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        return config_data

    def create_instance(module_and_class_name: str, args: list = [], kwargs: dict = {}) -> any:

        """
        Example call: 
        kmeans = create_instance(algorithms_config.get('ml_algorithm'),
                        args = algorithms_config.get('ml_algorithm_args', []), 
                        kwargs = algorithms_config.get('ml_algorithm_kwargs', {})
                 )

        YAML:
        ml_algorithm: sklearn.cluster.KMeans
        ml_algorithm_kwargs:
            n_clusters: 5
            random_state: 42
        """

        module_name, class_name = module_and_class_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)

        # Call the constructor with the appropriate combination of arguments
        if args and kwargs:
            return cls(*args, **kwargs)
        elif args:
            return cls(*args)
        elif kwargs:
            return cls(**kwargs)
        else:  # No arguments
            return cls() 

    def read_local(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            return df
        except FileNotFoundError as e:
            raise
        except pd.errors.EmptyDataError as e:
            raise
        except Exception as e:
            raise

    def save_model(self, model: any, local_path: str, format: str = 'pickle') -> None:
        try:
            if format == 'joblib':
                dump(model, local_path)
            elif format == 'pickle':
                with open(local_path, 'wb') as f:
                    pickle.dump(model, f)
            else:
                raise Exception('Parâmetro format [inválido], deve ser "joblib" ou "pickle"')
        except Exception as e:
            raise e

    def load_model(self, local_path: str, format: str = 'pickle') -> any:
        try:
            model: any = None
            if format == 'joblib':
                model = load(model, local_path)
            elif format == 'pickle':
                with open(local_path, 'rb') as f:
                    model = pickle.load(f)
            else:
                raise Exception('Parâmetro format [inválido], deve ser "joblib" ou "pickle"')
            
            return model
        except Exception as e:
            raise e

    def read_azure_blob_dataframe(self, blob_name: str) -> pd.DataFrame:
        try:
            
            # Download the blob as a string
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().content_as_text()

            # Convert the string data into a pandas DataFrame
            csv_data = StringIO(blob_data)
            df = pd.read_csv(csv_data)
            return df
        except FileNotFoundError as e:
            raise
        except pd.errors.EmptyDataError as e:
            raise
        except Exception as e:
            raise
        
    def write_azure_blob_dataframe(self, df: pd.DataFrame, blob_name: str) -> None:
        """
        Writes a Pandas DataFrame directly to Azure Blob Storage as a CSV file.
        
        :param df: DataFrame to upload.
        :param blob_name: Name of the blob where the CSV will be stored.
        """
        try:
            # Convert DataFrame to CSV format in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)  # Move cursor to the start of the stream
            
            # Get the blob client
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Upload the CSV data from the buffer
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        
        except Exception as e:
            print(f"Failed to write DataFrame to Azure Blob: {e}")
            raise

    def read_azure_blob_text(self, blob_name: str) -> str:
        try:
            
            # Download the blob as a string
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().content_as_text()
            return blob_data
        
        except FileNotFoundError as e:
            raise
        except pd.errors.EmptyDataError as e:
            raise
        except Exception as e:
            raise

    def read_azure_blob_file(self, blob_name: str, local_file_path: str) -> None:
        try:
            # Download the blob as a string
            blob_client = self.container_client.get_blob_client(blob_name)

            with open(local_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
                
        except FileNotFoundError as e:
            raise
        except pd.errors.EmptyDataError as e:
            raise
        except Exception as e:
            raise
    
    def read_azure_blob_binary(self, blob_name: str) -> bytes:
        try:
            
            # Download the blob as a string
            blob_client = self.container_client.get_blob_client(blob_name)
            download_stream = blob_client.download_blob()
            binary_content = download_stream.readall()
            
            return binary_content
        
        except FileNotFoundError as e:
            raise
        except pd.errors.EmptyDataError as e:
            raise
        except Exception as e:
            raise

    def write_azure_blob(self, remote_path_name: str, local_path_name: str) -> None:
        try:
            
            # Download the blob as a string
            blob_client = self.container_client.get_blob_client(remote_path_name)
            
            with open(local_path_name, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)  

        except Exception as e:
            raise
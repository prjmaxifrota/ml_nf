import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.sql_storage import ConnectionType, SqlStorage
from services.file_services import FileServices

analytics_endpoint = "rlqqphyufm2e7g2qka46krw7xi-qnlc7nyhjl7u7km24oymwac3ye.datawarehouse.fabric.microsoft.com"
database = "hub_frota_lakehouse"
sample_size = "1000"

# Create an instance of FabricLakehouseQueryExecutor
executor = None
error = False
try:
    executor = SqlStorage(analytics_endpoint, database="hub_frota_lakehouse", connection_type=ConnectionType.FABRIC_LAKEHOUSE)
except Exception as e:
    error = True
    print(f"\nError connecting to the lakehouse: {e}, exit.")

if not error:
    print("\nConnected to the lakehouse successfully.")

    files = FileServices()

    project_root = os.getcwd()

    query_path = os.path.join(project_root, 'analysis_generators', 'sql', 'trn.sql') 
    # query
    query = files.read_from_file(query_path)
    query = query.replace('{sample_size}', sample_size) 

    # Local path and filename for saving the CSV
    workspace_name = 'trn_investiga'
    local_path = os.path.join(project_root, 'analysis_generators', 'data', 'result')
    csv_filename = f"trn_{sample_size}_rows.csv"

    # Execute the query and save results
    #executor.query_and_save(query, local_path, csv_filename)

    df = executor.execute_query(query)
    print(len(df))
    print(df[['modelo', 'estabelecimento', 'custo_km']].head(20))
    
    executor.close()
    
    print("Command executed successfully.")

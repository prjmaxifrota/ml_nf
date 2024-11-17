import os, sys
from populator import Populator
from dotenv import load_dotenv
load_dotenv()

runner = Populator()

project_root = os.getcwd()
experiment_name = 'ml_busca_nf'
gen_path = os.path.join(project_root, 'experiments', 'data')

try:
    params = {
        'experiment_name': experiment_name,
        'df_analysis': None,
        'gen_path': gen_path,
        'sql_query': os.getenv('TRN_BUSCA_NF_SQL', 'trn_ml_busca_nf.sql'),
        'sample_size': 1000,
        'save_results_to_db': False,
        'refresh_source': 'sql', # sql, azure_csv     
        'csv_remote_name': f'trn_{experiment_name}.csv',
    }

    runner.initialize(params)

    if runner.df_analysis is not None:
        print(f'Returned {len(runner.df_analysis)}')
        print(runner.df_analysis.head(200))
    else: 
        print('No data returned.')

except Exception as e:
    print(f"SQL Connection error {str(e)}")

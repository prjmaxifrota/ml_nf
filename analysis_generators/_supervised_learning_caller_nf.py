import pandas as pd
import os, sys
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis_generators._supervised_learning_consumer import SupervisedLearningConsumer
import csv 

# Setting up project root and paths
project_root = os.getcwd()
csv_path = os.path.join(project_root, 'experiments', 'data', 'trn_ml_busca_nf.csv')

gen_path = os.path.join(project_root, 'analysis_generators', 'results_analysis')

# Load data
df_analysis = pd.read_csv(csv_path)

# Parameter setup
cat_cols = ['nota_fiscal_id', 'status', 'nome_serv']
numerical_col = 'valor'

cols_for_df = cat_cols + [numerical_col] 
sumary_cols = cols_for_df + ['consensus_count', 'model_agreement', 
                             'trend_detected', 'relationship', 'performance_reliability', 'ml_weight_score', 'description', 'action_summary']
# Create an empty list to collect interpretation DataFrames
interpreted_dfs = []

# Instantiate the consumer (outside loop to avoid unnecessary instantiation)
consumer = None
start_time_main = time.time()
print(f'Starting workflow for all columns...\n')
for i, target_col in enumerate(cat_cols):
    # Prepare categorical columns for the current iteration
    cat_cols_run = [col for col in cat_cols if col != target_col]

    # Instantiate a new consumer for each iteration with a fresh copy of df_analysis
    consumer = SupervisedLearningConsumer(df_analysis.copy(), categorical_cols=cat_cols_run, 
                                          numerical_cols=[numerical_col], target_col=target_col)

    # Run the workflow
    start_time = time.time()
    print(f'Starting the workflow for target column: {target_col}...')
    
    # Interpret the result
    df_interpreted = consumer.run_workflow()
    df_interpreted = df_interpreted[sumary_cols]

    cols_on_the_run = '-'.join(cat_cols_run)
    df_interpreted['feature_context'] = cols_on_the_run
    df_interpreted['target_feature'] = target_col

    # Save the interpreted DataFrame to a CSV file
    #file_name = f'{cols_on_the_run}_({target_col}).csv'
    #df_interpreted.to_csv(os.path.join(gen_path, file_name), index=True, quoting=csv.QUOTE_NONE)

    # Append the result to the list
    interpreted_dfs.append(df_interpreted[sumary_cols + ['feature_context', 'target_feature']])
    
    print(f'Finished the workflow for target column: {target_col} in {time.time() - start_time:.2f} seconds.\n')

# Concatenate all interpreted DataFrames into one
print(f'Finished main workflow in {time.time() - start_time_main:.2f} seconds.\n')
final_interpretation_df = pd.concat(interpreted_dfs, ignore_index=False)


# Optionally save or process the final combined DataFrame
final_file_name = 'ml_' + '-'.join(cat_cols) + '.csv'
final_interpretation_df.to_csv(os.path.join(gen_path, final_file_name), index=True, quoting=csv.QUOTE_NONE, sep=";")
print("All interpretations have been processed and saved.")

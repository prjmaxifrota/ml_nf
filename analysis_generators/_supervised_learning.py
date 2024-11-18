import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import os, sys
import time
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import RidgeClassifier
from sklearn.linear_model import SGDClassifier
from concurrent.futures import ThreadPoolExecutor
from sklearn.metrics import accuracy_score

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis_generators._row_classifier import RowClassifier

class SupervisedLearning:

    def __init__(self, df=None, categorical_cols=[], 
                 numerical_cols=[], target_col=None, id_col=None):
        
        self.id_col = id_col
        
        self.setup_context(df, categorical_cols, numerical_cols, target_col)
            
    def setup_context(self, df, categorical_cols, numerical_cols, target_col):
        
        self.df = df.copy()  # Make a copy of df to avoid modifying the original

        # Set the index on the copy to ensure original df remains unchanged
        if self.id_col in self.df.columns:
            self.df = self.df.set_index(self.id_col)
        
        self.target_col = target_col
        self.X = self.df.drop(columns=[target_col])
        self.y = self.df[target_col]
        self.categorical_cols = categorical_cols
        self.numerical_cols = numerical_cols
        self.row_classifier = RowClassifier()

        # Splitting data into training and testing sets
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(self.X, self.y, test_size=0.3, random_state=42)

    def perform_logistic_regression(self, df):
        # Define the preprocessor
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), self.numerical_cols),
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), self.categorical_cols)
            ]
        )

        # Set up pipeline with preprocessing and LogisticRegression
        model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', LogisticRegression(max_iter=1000, random_state=42))
        ])
        
        # Fit the model
        model.fit(self.X_train, self.y_train)
        
        # Predict on the full dataset
        df['logistic_regression_pred'] = model.predict(self.X)

        return df


    def perform_ridge_classifier(self, df):
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), self.categorical_cols)
            ]
        )

        # Set up pipeline with preprocessing and RidgeClassifier
        model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RidgeClassifier())
        ])
        
        # Fit the model
        model.fit(self.X_train, self.y_train)

        # Predict on the full dataset
        df['ridge_classifier_pred'] = model.predict(self.X)

        return df
    
    def perform_sgd_classifier(self, df):
        # Define preprocessor for categorical columns
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), self.categorical_cols)
            ]
        )

        # Set up pipeline with preprocessor and SGDClassifier
        model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', SGDClassifier(loss='log_loss', max_iter=1000, tol=1e-3, random_state=42))
        ])

        # Fit the model
        model.fit(self.X_train, self.y_train)

        # Predict on the full dataset
        df['sgd_classifier_pred'] = model.predict(self.X)

        return df

    def run_workflow(self):
        # Define each method and create independent copies of df
        tasks = [
            ("perform_ridge_classifier", self.perform_ridge_classifier, self.df.copy()),
            ("perform_sgd_classifier", self.perform_sgd_classifier, self.df.copy()),
            ("perform_logistic_regression", self.perform_logistic_regression, self.df.copy())
        ]

        # Dictionary to store results and completion times
        results = {}
        
        # Run each method in parallel, recording accurate times for each
        with ThreadPoolExecutor() as executor:
            futures = {}
            
            # Submit each task and store its start time
            for name, func, df in tasks:
                futures[name] = (executor.submit(run_with_timing, func, df), time.time())

            # Retrieve each result, timing each accurately
            for name, (future, start_time) in futures.items():
                result, duration = future.result()
                results[name] = result  # Store the result
                print(f'# Completed {name} in {duration:.2f} seconds')
        
        # Unpack results for further workflow processing
        df_ridge = results["perform_ridge_classifier"]
        df_sgd = results["perform_sgd_classifier"]
        df_lr = results["perform_logistic_regression"]

        return df_ridge, df_sgd, df_lr

    def run_workflow_sequencial(self):
        
        # perform_ridge_classifier
        start_time = time.time()
        print('Starting perform_ridge_classifier...')
        df_ridge = self.perform_ridge_classifier(self.df.copy())
        print(f'Completed perform_ridge_classifier in {time.time() - start_time:.2f} seconds')

        # perform_sgd_classifier
        start_time = time.time()
        print('Starting perform_sgd_classifier...')
        df_sgd = self.perform_sgd_classifier(df_ridge.copy())
        print(f'Completed perform_sgd_classifier in {time.time() - start_time:.2f} seconds')

        # perform_logistic_regression
        start_time = time.time()
        print('Starting perform_logistic_regression...')
        df_lr = self.perform_logistic_regression(df_sgd.copy())
        print(f'Completed perform_logistic_regression in {time.time() - start_time:.2f} seconds')
        
        # Return the DataFrame with all predictions added
        return df_ridge, df_sgd, df_lr

    def interpret(self, original_df, report_ridge_df, report_sgd_df, report_logistic_regression_df):
        """
        Cross-validate model reports to validate labels, detect trends, and identify relationships.
        Returns an interpretation DataFrame.
        """
        
        report_ridge_df = report_ridge_df.reindex(original_df.index)
        report_sgd_df = report_sgd_df.reindex(original_df.index)
        report_logistic_regression_df = report_logistic_regression_df.reindex(original_df.index)
        
         # Create a combined DataFrame with model predictions
        combined_df = original_df.copy()
        combined_df['ridge_pred'] = report_ridge_df['ridge_classifier_pred']
        combined_df['sgd_pred'] = report_sgd_df['sgd_classifier_pred']
        combined_df['logistic_regression_pred'] = report_logistic_regression_df['logistic_regression_pred']
        
        # Validate labels: Create columns to compare predictions with actual labels
        combined_df['ridge_correct'] = combined_df[self.target_col] == combined_df['ridge_pred']
        combined_df['sgd_correct'] = combined_df[self.target_col] == combined_df['sgd_pred']
        combined_df['lr_correct'] = combined_df[self.target_col] == combined_df['logistic_regression_pred']

        # Calculate consensus: How many models agree on a prediction
        combined_df['consensus_count'] = (
            (combined_df['ridge_pred'] == combined_df['sgd_pred']).astype(int) +
            (combined_df['ridge_pred'] == combined_df['logistic_regression_pred']).astype(int) +
            (combined_df['sgd_pred'] == combined_df['logistic_regression_pred']).astype(int) 
        )

        # Calculate model agreement: number of models agreeing on the most frequent prediction
        combined_df['model_agreement'] = combined_df[[
            'ridge_pred', 
            'sgd_pred', 
            'logistic_regression_pred'
        ]].apply(lambda row: row.value_counts().iloc[0], axis=1)

        # Detect trends and relationships
        combined_df['trend_detected'] = combined_df.apply(
            lambda row: 'Consistent Trend' if row['consensus_count'] >= 3 else 'Inconsistent Trend', axis=1
        )

        # Identify any obscure relationships
        combined_df['relationship'] = combined_df.apply(
            lambda row: 'Potential Anomaly' if not row['ridge_correct'] and not row['sgd_correct'] 
                        and not row['lr_correct'] 
                        else 'Clear Relationship', axis=1
        )
        
        # Calculate accuracy for each model by comparing predictions with the target column
        combined_df['ridge_accuracy'] = combined_df.apply(lambda row: accuracy_score([row[self.target_col]], [row['ridge_pred']]), axis=1)
        combined_df['sgd_accuracy'] = combined_df.apply(lambda row: accuracy_score([row[self.target_col]], [row['sgd_pred']]), axis=1)
        combined_df['lr_accuracy'] = combined_df.apply(lambda row: accuracy_score([row[self.target_col]], [row['logistic_regression_pred']]), axis=1)
        
        # Calculate the average accuracy to represent performance reliability
        combined_df['performance_reliability'] = combined_df[['ridge_accuracy', 'sgd_accuracy', 'lr_accuracy']].mean(axis=1)

        # Add the `target_col` column to track which feature was the target in each run
        combined_df['target_col'] = self.target_col

        # Apply classify_ml_row and store the classification results
        def classify_ml_row(row):
            # Get the classification details for each row
            classification_result = self.row_classifier.classify_ml_row(row)
            
            # Use get_ml_description to fetch the human-readable description
            description = self.row_classifier.get_description(
                classification_result['description_code'], lang='pt-BR', type='ml' 
            )
            
            # Store the classification results in the row
            row['ml_weight_score'] = classification_result['ml_weight_score']
            row['description_code'] = classification_result['description_code']
            row['ml_good_or_bad'] = classification_result['ml_good_or_bad']
            row['description'] = description
            
            return row

        # Apply the classify_ml_row function to each row in combined_df
        combined_df = combined_df.apply(classify_ml_row, axis=1)
        
        # Recommendation for replacement
        combined_df['recomend_replacement'] = combined_df.apply(
            lambda row: 'yes' if (
                row['performance_reliability'] < 0.5 or
                row['trend_detected'] == 'Inconsistent Trend' or
                row['relationship'] == 'Potential Anomaly' or
                row['ml_weight_score'] < 0  # Replace based on negative weight score
            ) else 'no', axis=1
        )
        
        # Round all numerical columns to 2 decimal places (excluding 'original_id')
        combined_df = combined_df.apply(lambda x: round(x, 2) if np.issubdtype(x.dtype, np.number) else x)
        
        def generate_action_summary(row):
            if row['recomend_replacement'] == 'yes':
                # Recomenda substituição com base em baixa confiabilidade ou anomalias críticas
                if row['ml_good_or_bad'] == 'bad':
                    return "Sugerir substituição imediata devido a problemas críticos na confiabilidade e anomalias."
                elif row['performance_reliability'] < 0.5:
                    return "Sugerir substituição devido à baixa confiabilidade, desempenho e tendências inconsistentes."
                else:
                    return "Revisar substituição devido a problemas moderados de confiabilidade ou consistência."

            elif row['trend_detected'] == 'Inconsistent Trend' or row['relationship'] == 'Potential Anomaly':
                # Investigação de possíveis anomalias ou inconsistências
                if row['model_agreement'] < 2:
                    return "Sugerir investigação devido a potenciais inconsistências."
                else:
                    return "Investigar possíveis inconsistências ou anomalias detectadas nas tendências."

            elif row['performance_reliability'] >= 0.85 and row['model_agreement'] >= 3:
                # Alta confiança com forte confiabilidade
                return "Alta previsibilidade: continuar com a abordagem atual."

            elif row['performance_reliability'] >= 0.7 and row['trend_detected'] == 'Consistent Trend':
                # Bom desempenho e consistência, mas não no nível mais alto
                return "Bom desempenho: continuar com ação mínima. Monitorar para quaisquer mudanças."

            elif 0.6 <= row['performance_reliability'] < 0.7:
                # Desempenho borderline; recomendar monitoramento
                return "Desempenho satisfatório porém no limite. Monitorar de perto e ajustar se o desempenho piorar."

            elif row['ml_good_or_bad'] == 'bad' and row['performance_reliability'] < 0.6:
                # Baixa confiabilidade e classificação ruim do modelo
                return "Sugerir substituição devido à baixa previsibilidade."

            else:
                # Caso padrão para monitoramento e ajustes menores
                return "Monitorar desempenho e consistência."

        # Apply the action summary function to each row
        combined_df['action_summary'] = combined_df.apply(generate_action_summary, axis=1)

        return combined_df


def run_with_timing(func, df):
    start_time = time.time()
    result = func(df)
    elapsed_time = time.time() - start_time
    return result, elapsed_time
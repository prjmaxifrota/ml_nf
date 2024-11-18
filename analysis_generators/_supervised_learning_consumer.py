import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis_generators._supervised_learning import SupervisedLearning
import time
import pandas as pd
project_root = os.getcwd()

class SupervisedLearningConsumer:

    def __init__(self, df=None, categorical_cols=[], 
                 numerical_cols=[], target_col=None, id_col=None):
        
        self.df = df
        self.target_col = target_col
        self.categorical_cols = categorical_cols 
        self.numerical_cols = numerical_cols 
        self.supervised_learner = SupervisedLearning(self.df, 
                                                     categorical_cols=self.categorical_cols, 
                                                     numerical_cols=self.numerical_cols,
                                                     target_col=self.target_col, id_col=id_col)

    def run_workflow(self):
        # Run all models and return the final DataFrame with predictions
        df_ridge, df_sgd, df_lr = self.supervised_learner.run_workflow()
        start_time = time.time()
        print('Starting ML interpretation...')
        df_interpreted = self.supervised_learner.interpret(self.df, df_ridge, df_sgd, df_lr)
        print(f'ML interpretation completed in {time.time() - start_time:.2f} seconds.')
        return df_interpreted

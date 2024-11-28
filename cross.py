import pandas as pd
import numpy as np
from geopy.distance import geodesic

import os, sys
from populator import Populator
from dotenv import load_dotenv
load_dotenv()

from services.file_services import FileServices

def clean_coordinates(df):
    return df.dropna(subset=['latitude', 'longitude'])

# Function to calculate distances
def calculate_distances(df1, df2):
    closest_distances_df1 = []
    closest_distances_df2 = []
    
    # Iterate over rows in df1
    for _, row1 in df1.iterrows():
        distances = df2.apply(
            lambda row2: geodesic((row1['latitude'], row1['longitude']), 
                                  (row2['latitude'], row2['longitude'])).km, axis=1)
        closest_distances_df1.append(distances.min())
    
    # Iterate over rows in df2
    for _, row2 in df2.iterrows():
        distances = df1.apply(
            lambda row1: geodesic((row2['latitude'], row2['longitude']), 
                                  (row1['latitude'], row1['longitude'])).km, axis=1)
        closest_distances_df2.append(distances.min())
    
    # Add distances as new columns
    df1['closest_distance'] = closest_distances_df1
    df2['closest_distance'] = closest_distances_df2
    
    return df1, df2

project_root = os.getcwd()
experiment_name = 'ml_creds-2'
p1 = os.path.join(project_root, 'experiments', 'data', 'trn_ml_creds-2.csv')
p2 = os.path.join(project_root, 'experiments', 'data', 'trn_ml_busca_nf_v3_large.csv')

files = FileServices()

c1 = files.count_csv_rows(p1)
print(f'CSV Credenciados: {c1} rows')

c2 = files.count_csv_rows(p2)
print(f'TRN: {c2} rows')

# Call the function
df1 = pd.read_csv(p1, nrows=100)
df1 = clean_coordinates(df1[['latitude', 'longitude']])
df2 = pd.read_csv(p2, nrows=100)
df2 = clean_coordinates(df2[['latitude', 'longitude']])
df1, df2 = calculate_distances(df1, df2)

print(df1[['closest_distance']].head())
print(df2[['closest_distance']].head())



#!/usr/bin/env python3
"""
Convert PBC data from pbc.dat.txt to CSV format for easy pandas loading.
"""

import pandas as pd

def convert_pbc_to_csv():
    """Convert PBC data to CSV with proper column headers."""
    
    # Column definitions with descriptions
    columns = [
        'id',          # Patient ID
        'futime',      # Follow-up time in days (survival time)
        'status',      # 0=alive, 1=transplant, 2=dead
        'drug',        # 1=D-penicillamine, 2=placebo
        'age',         # Age in days
        'sex',         # 0=male, 1=female
        'ascites',     # 0=no, 1=yes
        'hepato',      # Hepatomegaly: 0=no, 1=yes
        'spiders',     # Spider angiomata: 0=no, 1=yes
        'edema',       # 0=none, 0.5=moderate, 1=severe
        'bili',        # Bilirubin mg/dl
        'chol',        # Cholesterol mg/dl
        'albumin',     # Albumin gm/dl
        'copper',      # Urine copper ug/day
        'alk_phos',    # Alkaline phosphatase U/liter
        'sgot',        # SGOT U/ml
        'trig',        # Triglycerides mg/dl
        'platelet',    # Platelets per cubic ml/1000
        'protime',     # Prothrombin time seconds
        'stage'        # Disease stage 1-4
    ]
    
    # Read the data
    print("Reading pbc.dat.txt...")
    df = pd.read_csv('pbc.dat.txt', sep=r'\s+', header=None, names=columns, na_values='.')
    
    print(f"Loaded {len(df)} patients with {len(df.columns)} features")
    print(f"Missing values per column:")
    print(df.isnull().sum())
    
    # Save as CSV
    output_file = 'pbc_data.csv'
    df.to_csv(output_file, index=False)
    print(f"Saved to {output_file}")
    
    # Create a version with derived features for ML
    df_ml = df.copy()
    
    # Convert age from days to years
    df_ml['age_years'] = df_ml['age'] / 365.25
    
    # Create death event indicator for survival analysis
    df_ml['death_event'] = (df_ml['status'] == 2).astype(int)
    
    # Create binary features
    df_ml['male'] = (df_ml['sex'] == 0).astype(int)
    df_ml['female'] = (df_ml['sex'] == 1).astype(int)
    df_ml['drug_treatment'] = (df_ml['drug'] == 1).astype(int)
    
    # Save ML-ready version
    ml_output_file = 'pbc_ml_ready.csv'
    df_ml.to_csv(ml_output_file, index=False)
    print(f"Saved ML-ready version to {ml_output_file}")
    
    return df

if __name__ == "__main__":
    convert_pbc_to_csv()
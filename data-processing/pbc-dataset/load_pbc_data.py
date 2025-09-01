#!/usr/bin/env python3
"""
Simple pandas data loader for PBC dataset - ready for ML workflows.
"""

import pandas as pd
import numpy as np

def load_pbc_data(csv_path: str = 'pbc_ml_ready.csv') -> pd.DataFrame:
    """
    Load PBC data for machine learning workflows.
    
    Args:
        csv_path: Path to CSV file (default: pbc_ml_ready.csv)
        
    Returns:
        DataFrame ready for ML with proper dtypes and clean missing values
    """
    df = pd.read_csv(csv_path)
    
    # Convert categorical variables to proper types
    categorical_cols = ['drug', 'sex', 'ascites', 'hepato', 'spiders', 'stage', 
                       'death_event', 'male', 'female', 'drug_treatment']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('Int64')  # Nullable integer
    
    return df

def get_survival_data(df: pd.DataFrame) -> tuple:
    """
    Extract survival analysis data (time, event) for ML models.
    
    Args:
        df: PBC DataFrame
        
    Returns:
        Tuple of (survival_times, death_events, feature_matrix)
    """
    # Survival times and events
    T = df['futime'].values  # Time to event in days
    E = df['death_event'].values  # 1=death, 0=censored
    
    # Feature matrix (exclude survival outcomes and derived columns)
    feature_cols = ['age_years', 'male', 'ascites', 'hepato', 'spiders', 'edema',
                   'bili', 'chol', 'albumin', 'copper', 'alk_phos', 'sgot', 
                   'trig', 'platelet', 'protime', 'stage', 'drug_treatment']
    
    X = df[feature_cols].copy()
    
    # Handle missing values (simple median imputation for demo)
    X = X.fillna(X.median())
    
    return T, E, X

def get_feature_descriptions() -> dict:
    """
    Get detailed descriptions of all features for ML interpretability.
    
    Returns:
        Dictionary mapping feature names to descriptions
    """
    return {
        'futime': 'Follow-up time in days (survival outcome)',
        'death_event': 'Death indicator (1=died, 0=alive/transplant)',
        'age_years': 'Patient age in years',
        'male': 'Male gender (1=male, 0=female)',
        'ascites': 'Ascites present (fluid in abdomen)',
        'hepato': 'Hepatomegaly present (enlarged liver)',
        'spiders': 'Spider angiomata present (vascular lesions)',
        'edema': 'Edema severity (0=none, 0.5=moderate, 1=severe)',
        'bili': 'Serum bilirubin mg/dl (liver function)',
        'chol': 'Serum cholesterol mg/dl',
        'albumin': 'Serum albumin gm/dl (liver synthetic function)',
        'copper': '24-hour urine copper ug/day',
        'alk_phos': 'Alkaline phosphatase U/liter (bile duct function)',
        'sgot': 'SGOT/AST U/ml (liver enzyme)',
        'trig': 'Triglycerides mg/dl',
        'platelet': 'Platelet count per cubic ml/1000',
        'protime': 'Prothrombin time seconds (clotting function)',
        'stage': 'Histologic disease stage (1-4)',
        'drug_treatment': 'D-penicillamine treatment (1=yes, 0=placebo)'
    }

def print_dataset_summary(df: pd.DataFrame) -> None:
    """Print summary statistics for the PBC dataset."""
    print(f"\nPBC Dataset Summary:")
    print(f"Total patients: {len(df)}")
    print(f"Deaths: {df['death_event'].sum()}")
    print(f"Transplants: {(df['status'] == 1).sum()}")
    print(f"Alive: {(df['status'] == 0).sum()}")
    print(f"Median survival time: {df['futime'].median():.0f} days")
    print(f"Age range: {df['age_years'].min():.1f} - {df['age_years'].max():.1f} years")
    print(f"Missing values per column:")
    print(df.isnull().sum()[df.isnull().sum() > 0])

if __name__ == "__main__":
    # Example usage
    print("Converting PBC data to CSV...")
    
    # First convert the raw data
    import subprocess
    subprocess.run(['python', 'convert_pbc_to_csv.py'])
    
    # Then load and summarize
    df = load_pbc_data()
    print_dataset_summary(df)
    
    # Get survival data for ML
    T, E, X = get_survival_data(df)
    print(f"\nML-ready data shapes:")
    print(f"Survival times: {T.shape}")
    print(f"Death events: {E.shape}")
    print(f"Feature matrix: {X.shape}")
    print(f"Features: {list(X.columns)}")
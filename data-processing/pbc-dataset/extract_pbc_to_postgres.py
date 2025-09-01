#!/usr/bin/env python3
"""
Extract PBC data from pbc.dat.txt and write to PostgreSQL database.
Primary Biliary Cirrhosis (PBC) dataset for liver transplant allocation research.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

def read_pbc_data(file_path: str) -> pd.DataFrame:
    """
    Read PBC data from space-separated text file.
    
    The dataset contains 20 columns representing clinical variables for PBC patients:
    - futime: Days from registration to death/transplant/study end (survival outcome)
    - status: Patient outcome (0=alive, 1=transplant, 2=death)
    - Clinical labs: bili, albumin, protime (similar to MELD components)
    - Disease indicators: ascites, edema, stage (disease severity)
    - Demographics: age, sex
    - Other markers: various liver function tests and clinical signs
    
    Args:
        file_path: Path to pbc.dat.txt file
        
    Returns:
        DataFrame with PBC patient data
    """
    column_names = [
        'id', 'futime', 'status', 'drug', 'age', 'sex', 'ascites', 
        'hepato', 'spiders', 'edema', 'bili', 'chol', 'albumin', 
        'copper', 'alk_phos', 'sgot', 'trig', 'platelet', 'protime', 'stage'
    ]
    
    df = pd.read_csv(file_path, sep=r'\s+', header=None, names=column_names, na_values='.')
    return df

def load_schema_from_file(cursor, schema_file: str) -> None:
    """
    Load database schema from SQL file.
    
    Args:
        cursor: PostgreSQL database cursor
        schema_file: Path to SQL schema file
    """
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    
    # Execute each statement separately (split by semicolon)
    statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
    for statement in statements:
        cursor.execute(statement)

def insert_pbc_data(cursor, df: pd.DataFrame) -> None:
    """
    Insert PBC data into PostgreSQL table.
    
    Args:
        cursor: PostgreSQL database cursor
        df: DataFrame containing PBC data
    """
    # Replace pandas NaN with None for PostgreSQL NULL
    df_clean = df.fillna(None)
    
    insert_sql = """
    INSERT INTO pbc_patients (
        id, futime, status, drug, age, sex, ascites, hepato, spiders, 
        edema, bili, chol, albumin, copper, alk_phos, sgot, trig, 
        platelet, protime, stage
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        futime = EXCLUDED.futime,
        status = EXCLUDED.status,
        drug = EXCLUDED.drug,
        age = EXCLUDED.age,
        sex = EXCLUDED.sex,
        ascites = EXCLUDED.ascites,
        hepato = EXCLUDED.hepato,
        spiders = EXCLUDED.spiders,
        edema = EXCLUDED.edema,
        bili = EXCLUDED.bili,
        chol = EXCLUDED.chol,
        albumin = EXCLUDED.albumin,
        copper = EXCLUDED.copper,
        alk_phos = EXCLUDED.alk_phos,
        sgot = EXCLUDED.sgot,
        trig = EXCLUDED.trig,
        platelet = EXCLUDED.platelet,
        protime = EXCLUDED.protime,
        stage = EXCLUDED.stage;
    """
    
    values = [tuple(row) for row in df_clean.values]
    execute_values(cursor, insert_sql, values)

def main():
    """
    Main function to extract PBC data and write to PostgreSQL.
    
    Process:
    1. Read PBC data from pbc.dat.txt (space-separated format)
    2. Load database schema with detailed column descriptions
    3. Connect to PostgreSQL database
    4. Create tables and indexes
    5. Insert patient data with upsert logic
    """
    # Database connection parameters from environment variables
    db_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'liver_transplant'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    pbc_file = 'pbc.dat.txt'
    schema_file = 'pbc_schema.sql'
    
    try:
        # Read PBC data
        print(f"Reading PBC data from {pbc_file}...")
        df = read_pbc_data(pbc_file)
        print(f"Loaded {len(df)} records with {len(df.columns)} columns")
        print(f"Survival time range: {df['futime'].min()}-{df['futime'].max()} days")
        print(f"Status distribution: {df['status'].value_counts().to_dict()}")
        
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {db_params['host']}:{db_params['port']}...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Load schema from file
        print(f"Loading database schema from {schema_file}...")
        load_schema_from_file(cursor, schema_file)
        
        # Insert data
        print("Inserting PBC data...")
        insert_pbc_data(cursor, df)
        
        # Commit changes
        conn.commit()
        print(f"Successfully inserted {len(df)} records into pbc_patients table")
        
        # Verify insertion and show sample statistics
        cursor.execute("SELECT COUNT(*) FROM pbc_patients")
        count = cursor.fetchone()[0]
        print(f"Total records in database: {count}")
        
        cursor.execute("""
        SELECT 
            COUNT(*) as total_patients,
            COUNT(CASE WHEN status = 2 THEN 1 END) as deaths,
            COUNT(CASE WHEN status = 1 THEN 1 END) as transplants,
            COUNT(CASE WHEN status = 0 THEN 1 END) as alive,
            ROUND(AVG(futime)) as avg_followup_days,
            ROUND(AVG(bili), 2) as avg_bilirubin
        FROM pbc_patients
        """)
        stats = cursor.fetchone()
        print(f"Dataset summary: {stats[0]} patients, {stats[1]} deaths, {stats[2]} transplants, {stats[3]} alive")
        print(f"Average follow-up: {stats[4]} days, Average bilirubin: {stats[5]} mg/dl")
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        print("Make sure pbc.dat.txt and pbc_schema.sql are in the current directory")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        print("Check PostgreSQL connection parameters and ensure database exists")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
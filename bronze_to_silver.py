import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import logging

# --- STEP 1: SETUP LOGGING ---
# This prints timestamps so we know when our script is running
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- STEP 2: LOAD SECRETS ---
# This looks for your .env file and pulls the password/username
load_dotenv()
db_pass = os.getenv('DB_PASSWORD')
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')

# --- STEP 3: CREATE THE DATABASE BRIDGE ---
# We use f-strings to build the URL securely using our variables
DB_URL = f'postgresql+psycopg2://{db_user}:{db_pass}@localhost/{db_name}'
engine = create_engine(DB_URL)

logging.info("Starting the Full Pipeline (Bronze -> Silver -> Gold)...")

try:
    # --- STEP 4: FETCH (BRONZE LAYER) ---
    logging.info("Step 1: Reading raw data from Bronze layer...")
    df = pd.read_sql("SELECT * FROM bronze.raw_jobs", engine)
    logging.info(f"Successfully fetched {len(df)} rows.")

    # --- STEP 5: TRANSFORM (SILVER LAYER) ---
    logging.info("Step 2: Cleaning and standardizing data...")
    
    # Standardize skills: lowercase and remove extra spaces
    df['skills'] = df['skills'].str.lower().str.strip()
    
    # Standardize roles: remove any accidental extra spaces
    df['role'] = df['role'].str.strip()
    
    # Save the cleaned data to the Silver schema
    logging.info("Step 3: Saving cleaned data to Silver layer...")
    df.to_sql('clean_jobs', engine, schema='silver', if_exists='replace', index=False)
    logging.info("SUCCESS: Silver Layer is ready!")

    # --- STEP 6: EXPLODE & ANALYSIS (GOLD LAYER) ---
    logging.info("Step 4: Generating Gold Layer (Splitting skills)...")
    
    # Split the "python, sql" string into a list ['python', 'sql']
    df['skill_list'] = df['skills'].str.split(',')
    
    # Explode turns 1 row with a list into multiple rows (one for each skill)
    df_gold = df.explode('skill_list')
    
    # Clean the skill name again just in case of spaces
    df_gold['skill_name'] = df_gold['skill_list'].str.strip()
    
    # Save the final analytical table to the Gold schema
    logging.info("Step 5: Saving to Gold layer...")
    df_gold[['job_id', 'skill_name', 'salary_lpa']].to_sql('skill_salary_analysis', engine, schema='gold', if_exists='replace', index=False)
    
    logging.info("GOLD SUCCESS: The entire pipeline is complete!")

except Exception as e:
    # If anything fails (wrong password, table missing), it shows up here
    logging.error(f"PIPELINE FAILED: {e}")
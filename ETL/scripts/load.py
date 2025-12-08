import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
#Supabase Client
def get_supabase_client():
    load_dotenv()
    url = os.getenv("url")
    key = os.getenv("key")
    if not url or not key:
        raise ValueError("Missing Supabase URL or Supabase Key in .env")
    return create_client(url, key)

# Load CSV into Supabase
def load_to_supabase(staged_path, table_name="iris_data"):
    staged_path = os.path.abspath(staged_path)
    print(f"Looking for file at: {staged_path}")
    if not os.path.exists(staged_path):
        print(f"ERROR: File not found at {staged_path}")
        return
    df = pd.read_csv(staged_path)
    df = df.where(pd.notnull(df), None)
    supabase = get_supabase_client()
    total_rows = len(df)
    batch_size = 50
    print(f"Uploading {total_rows} rows to Supabase table '{table_name}'...")
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size].to_dict(orient="records")
        try:
            supabase.table(table_name).insert(batch).execute()
            print(f"Inserted rows {i+1} to {min(i+batch_size, total_rows)}")
        except Exception as e:
            print(f"Batch error at {i // batch_size + 1}: {e}")
    print("Upload complete!")

#Main Execution
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_csv = os.path.join(base_dir,"data","staged","iris_transformed.csv")
    load_to_supabase(staged_csv)
import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Initialize supabase
load_dotenv()
supabase = create_client(os.getenv("supabase_url"), os.getenv("supabase_key"))

def load_to_supabase():
    csv_path = "../data/staged/cleaned_nasa_data.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file :{csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8")
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.strftime("%Y-%m-%d")
    df["extracted_at"] = pd.to_datetime(df.get("extracted_at"), errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    df = df.where(pd.notnull(df), None)
    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]
        prepared = []
        for r in batch_df.to_dict("records"):
            prepared.append({
                "date": r.get("date"),
                "explanation": r.get("explanation"),
                "hdurl": r.get("hdurl"),
                "media_type": r.get("media_type"),
                "service_version": r.get("service_version"),
                "title": r.get("title"),
                "url": r.get("url"),
                "extracted_at": r.get("extracted_at"),
            })
        if not prepared:
            continue
        res=supabase.table("nasa_data").insert(prepared).execute()
        if hasattr(res, "status_code") and res.status_code >= 400:
            raise RuntimeError(f"Supabase insert failed: {getattr(res, 'data', res)}")
        if isinstance(res, dict) and res.get("error"):
            raise RuntimeError(f"Supabase insert failed: {res['error']}")
        print(f"Inserted rows {i+1} to {min(i+batch_size, len(df))}")
        time.sleep(0.5)
    print("Finished Loading NASA APOD data")

if __name__ == "__main__":
    load_to_supabase()
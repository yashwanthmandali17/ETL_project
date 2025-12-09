import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Initialize supabase
load_dotenv()
supabase = create_client(os.getenv("url"), os.getenv("key"))

def load_to_supabase():
    # Load cleaned csv
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file : {csv_path}")
    df = pd.read_csv(csv_path)
    # Convert timestamps into strings
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    # Replace NaN with None so JSON serializes to null
    df = df.where(pd.notnull(df), None)
    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size].to_dict("records")
        prepared = []
        for r in batch:
            prepared.append({
                "time": r.get("time"),
                "temperature_c": r.get("temperature_C") if r.get("temperature_C") is not None else None,
                "humidity_percent": r.get("humidity_percent"),
                "wind_speed_kmph": r.get("wind_speed_kmph"),
                "city": r.get("city", "Hyderabad"),
                "extracted_at": r.get("extracted_at"),
            })
        # Insert batch into Supabase table "weather_data"
        res = supabase.table("weather_data").insert(prepared).execute()
        # Basic error check
        if hasattr(res, "status_code") and res.status_code >= 400:
            raise RuntimeError(f"Supabase insert failed: {res.data}")
        print(f"Inserted rows {i+1} to {min(i+batch_size, len(df))}")
        time.sleep(0.5)
    print("Finished Loading weather data")

if __name__ == "__main__":
    load_to_supabase()
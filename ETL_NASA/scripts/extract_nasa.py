import json
from pathlib import Path
import requests
from datetime import datetime

data_dir=Path(__file__).resolve().parents[1] / "data" / "raw"
data_dir.mkdir(parents=True, exist_ok=True)

def extract_weather_data(api_key, date):
    url="https://api.nasa.gov/planetary/apod"
    params={"api_key": api_key, "date": date}
    resp=requests.get(url, params=params)
    resp.raise_for_status()
    data=resp.json()
    filename=data_dir/f"nasa_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))
    print(f"Extracted nasa data saved to {filename}")

if __name__=="__main__":
    api_key="t7PgfYc3xZw7lZPSQLazaLKN0Hz7Oppf0Z61Kljk"
    date="2025-12-09"
    extract_weather_data(api_key,date)
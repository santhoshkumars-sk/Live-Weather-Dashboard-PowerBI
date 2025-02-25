import os
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import time

# Environment Variables
google_creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
spreadsheet_url = os.getenv("GOOGLE_SHEET_URL")

# Authenticate Google Sheets API
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(json.loads(google_creds_json), scopes=SCOPES)
gc = gspread.authorize(creds)

# Google Sheet Setup
spreadsheet = gc.open_by_url(spreadsheet_url)
try:
    worksheet = spreadsheet.worksheet("24_Hour_Data")
except gspread.exceptions.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet(title="24_Hour_Data", rows="50000", cols="10")

# Read city coordinates from CSV
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/Live-Weather-Dashboard-PowerBI/main/city_coordinates.csv"
districts_df = pd.read_csv(CSV_URL)
districts = list(zip(districts_df["Latitude"].round(4), districts_df["Longitude"].round(4), districts_df["City"]))

# Open-Meteo API URL Template
IST = pytz.timezone('Asia/Kolkata')
today_date = datetime.now(IST).strftime('%Y-%m-%d')
API_URL_TEMPLATE = (
    "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
    "&hourly=temperature_2m&start_date={date}&end_date={date}&timezone=Asia%2FKolkata"
)

HEADERS = ["City", "Latitude", "Longitude", "Timestamp", "Temperature (°C)"]

# Fetch data with retry mechanism (Exponential Backoff)
def fetch_today_data(lat, lon, city, retries=5, delay=2):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(API_URL_TEMPLATE.format(lat=lat, lon=lon, date=today_date), timeout=15)
            response.raise_for_status()
            data = response.json()

            timestamps = data.get("hourly", {}).get("time", [])
            temperatures = data.get("hourly", {}).get("temperature_2m", [])

            if not timestamps or not temperatures:
                print(f"⚠️ No data received for {city}")
                return []

            return [{
                "City": city,
                "Latitude": lat,
                "Longitude": lon,
                "Timestamp": ts,
                "Temperature (°C)": temp
            } for ts, temp in zip(timestamps, temperatures)]

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data for {city} (Attempt {attempt}/{retries}): {e}")
            time.sleep(delay * attempt)  # Exponential backoff

    print(f"🚨 Max retries reached for {city}. Skipping...")
    return []

# Fetch and save data for all cities
def fetch_all_cities_data():
    with ThreadPoolExecutor(max_workers=10) as executor:  # Reduce workers to prevent rate-limiting
        futures = {executor.submit(fetch_today_data, lat, lon, city): city for lat, lon, city in districts}

        all_data = []
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)
            else:
                print(f"⚠️ Skipped: No data for {futures[future]}")

    df = pd.DataFrame(all_data, columns=HEADERS)

    if not df.empty:
        print(f"📌 Saving {len(df)} records to 24_Hour_Data")
        worksheet.clear()
        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

if __name__ == "__main__":
    fetch_all_cities_data()

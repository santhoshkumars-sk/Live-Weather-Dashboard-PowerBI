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
creds_dict = json.loads(google_creds_json)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
gc = gspread.authorize(creds)

# Google Sheet
spreadsheet = gc.open_by_url(spreadsheet_url)
try:
    worksheet = spreadsheet.worksheet("24_Hour_Data")
except gspread.exceptions.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet(title="24_Hour_Data", rows="20000", cols="10")

# Read city coordinates from CSV
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/Live-Weather-Dashboard-PowerBI/main/city_coordinates.csv"
districts_df = pd.read_csv(CSV_URL)

# Prepare city lookup with consistent rounding for matching
city_lookup = {
    (round(row["Latitude"], 4), round(row["Longitude"], 4)): row["City"]
    for _, row in districts_df.iterrows()
}

# Generate a list of (lat, lon, city) tuples
districts = [(lat, lon, city_lookup[(lat, lon)]) for lat, lon in city_lookup.keys()]

# Open-Meteo API URL template for today's data
IST = pytz.timezone('Asia/Kolkata')
today_date = datetime.now(IST).strftime('%Y-%m-%d')
API_URL_TEMPLATE = (
    "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
    "&hourly=temperature_2m&start_date={date}&end_date={date}&timezone=Asia%2FKolkata"
)

HEADERS = ["City", "Latitude", "Longitude", "Timestamp", "Temperature (°C)"]

# Fetch data with retry mechanism
def fetch_today_data(lat, lon, city, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            url = API_URL_TEMPLATE.format(lat=lat, lon=lon, date=today_date)
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()

            timestamps = data.get("hourly", {}).get("time", [])
            temperatures = data.get("hourly", {}).get("temperature_2m", [])

            if not timestamps or not temperatures:
                print(f"⚠️ No data available for {city} on attempt {attempt + 1}.")
                return []

            return [{
                "City": city,
                "Latitude": lat,
                "Longitude": lon,
                "Timestamp": ts,
                "Temperature (°C)": temp
            } for ts, temp in zip(timestamps, temperatures)]

        except requests.exceptions.RequestException as e:
            attempt += 1
            print(f"❌ Request error for {city} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                print(f"🔄 Retrying in {delay} seconds...")
                time.sleep(delay)

    print(f"🚫 Failed to fetch data for {city} after {retries} attempts.")
    return []

# Fetch data for all cities
def fetch_all_cities_data():
    all_data = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_today_data, lat, lon, city) for lat, lon, city in districts]

        for future in as_completed(futures):
            city_data = future.result()
            if city_data:
                all_data.extend(city_data)

    total_expected_records = len(districts) * 24
    actual_records = len(all_data)

    if actual_records < total_expected_records:
        print(f"⚠️ Records fetched: {actual_records}/{total_expected_records} (Some data might be missing)")
    else:
        print(f"✅ All expected records fetched: {actual_records}")

    if not all_data:
        print("⚠️ No data fetched.")
        return

    df = pd.DataFrame(all_data, columns=HEADERS)
    worksheet.clear()
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
    print(f"✅ Data written to Google Sheets ({len(df)} records).")

if __name__ == "__main__":
    fetch_all_cities_data()

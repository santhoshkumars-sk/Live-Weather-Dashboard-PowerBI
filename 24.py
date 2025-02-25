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
    worksheet = spreadsheet.add_worksheet(title="24_Hour_Data", rows="1000", cols="10")

# Read city coordinates from CSV with column names
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/Live-Weather-Dashboard-PowerBI/main/city_coordinates.csv"
districts_df = pd.read_csv(CSV_URL)

# Create a dictionary for quick lookup of city names by (latitude, longitude)
city_lookup = {
    (round(row["Latitude"], 4), round(row["Longitude"], 4)): row["City"]
    for _, row in districts_df.iterrows()
}

# Prepare list of (lat, lon) tuples for API requests
districts = [(lat, lon) for lat, lon in city_lookup.keys()]

# Open-Meteo API URL template for today's data
IST = pytz.timezone('Asia/Kolkata')
today_date = datetime.now(IST).strftime('%Y-%m-%d')
API_URL_TEMPLATE = (
    "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
    "&hourly=temperature_2m&start_date={date}&end_date={date}&timezone=Asia%2FKolkata"
)

HEADERS = ["City", "Latitude", "Longitude", "Timestamp", "Temperature (°C)"]

# Fetch data for a single city using the city name from the CSV (via city_lookup)
def fetch_today_data(lat, lon):
    city = city_lookup.get((round(lat, 4), round(lon, 4)), "Unknown")
    try:
        url = API_URL_TEMPLATE.format(lat=lat, lon=lon, date=today_date)
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        timestamps = data.get("hourly", {}).get("time", [])
        temperatures = data.get("hourly", {}).get("temperature_2m", [])

        if not timestamps or not temperatures:
            print(f"⚠️ No data available for {city}.")
            return []

        return [{
            "City": city,
            "Latitude": lat,
            "Longitude": lon,
            "Timestamp": ts,
            "Temperature (°C)": temp
        } for ts, temp in zip(timestamps, temperatures)]

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error for {city}: {e}")
        return []

# Fetch data for all cities
def fetch_all_cities_data():
    all_data = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_coords = {
            executor.submit(fetch_today_data, lat, lon): (lat, lon)
            for lat, lon in districts
        }

        for future in as_completed(future_to_coords):
            lat, lon = future_to_coords[future]
            city = city_lookup.get((round(lat, 4), round(lon, 4)), "Unknown")
            try:
                data = future.result()
                all_data.extend(data)
                print(f"✅ Data fetched for {city} ({len(data)} records)")
            except Exception as e:
                print(f"❌ Error processing {city}: {e}")

    if not all_data:
        print("⚠️ No data fetched.")
        return

    df = pd.DataFrame(all_data, columns=HEADERS)
    worksheet.clear()
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
    print(f"✅ Data successfully written to Google Sheets ({len(df)} records).")

if __name__ == "__main__":
    fetch_all_cities_data()

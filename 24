import os
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
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
worksheet = spreadsheet.worksheet("24_Hour_Data")  # Create a separate sheet for 24-hour data

# Read city coordinates from CSV
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/Live-Weather-Dashboard-PowerBI/main/city_coordinates.csv"
districts_df = pd.read_csv(CSV_URL)
districts = districts_df[["Latitude", "Longitude", "City"]].values.tolist()

# Open-Meteo API URL template
API_URL_TEMPLATE = "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&past_days=1&timezone=Asia%2FKolkata"

HEADERS = ["City", "Latitude", "Longitude", "Timestamp", "Temperature (°C)"]

def fetch_24_hour_data(lat, lon, city):
    try:
        url = API_URL_TEMPLATE.format(lat=lat, lon=lon)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        timestamps = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]

        return [{
            "City": city,
            "Latitude": lat,
            "Longitude": lon,
            "Timestamp": ts,
            "Temperature (°C)": temp
        } for ts, temp in zip(timestamps, temperatures)]

    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
        return []

def fetch_all_cities_data():
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(lambda loc: fetch_24_hour_data(*loc), districts))

    # Flatten the list of lists
    all_data = [item for sublist in results for item in sublist]
    
    if not all_data:
        print("No data fetched.")
        return

    df = pd.DataFrame(all_data, columns=HEADERS)
    worksheet.clear()
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
    print("Data successfully written to Google Sheets.")

if __name__ == "__main__":
    fetch_all_cities_data()

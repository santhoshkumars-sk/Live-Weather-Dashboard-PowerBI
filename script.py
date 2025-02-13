import os
import json
import time
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from datetime import datetime, timedelta

# Loading Environment Variables
google_creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
spreadsheet_url = os.getenv("GOOGLE_SHEET_URL")
api_keys_env = os.getenv("OPENWEATHER_API_KEYS")

if not google_creds_json or not spreadsheet_url or not api_keys_env:
    raise ValueError("Missing required environment variables. Check your GitHub Secrets or local environment settings.")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Authenticate Google Sheets API
creds_dict = json.loads(google_creds_json)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
gc = gspread.authorize(creds)

# Loading Google Sheet
spreadsheet = gc.open_by_url(spreadsheet_url)
worksheet = spreadsheet.sheet1

# Loading CSV Data
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/weather-and-pollution-dashboard/main/city_coordinates.csv"
districts_df = pd.read_csv(CSV_URL)
districts = districts_df[["Latitude", "Longitude", "City"]].values.tolist()

# API Key 
api_keys = api_keys_env.split(",") 
api_key_cycle = cycle(api_keys)
key_usage = {key: 0 for key in api_keys}

# Columns
HEADERS = ["Latitude", "Longitude", "City", "Weather", "Weather Icon", "Temperature (°C)",
           "Pressure (hPa)", "Humidity (%)", "Visibility (km)", "Wind Speed (km/h)", "Wind Degree (°)",
           "Cloud Coverage (%)", "Sunrise", "Sunset", "AQI", "CO", "NO", "NO₂", "O₃", "SO₂", "PM2.5", "PM10", "NH₃", "Last Updated"]

# Function
def get_api_key():
    for _ in range(len(api_keys)):
        api_key = next(api_key_cycle)
        if key_usage[api_key] < 57:
            return api_key
    time.sleep(60)
    return next(api_key_cycle)

# Fetch Weather & Pollution Data
def fetch_data(lat, lon, city):
    for _ in range(3):
        api_key = get_api_key()
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        pollution_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"

        try:
            weather_response = requests.get(weather_url)
            pollution_response = requests.get(pollution_url)

            #API request errors
            if weather_response.status_code != 200 or pollution_response.status_code != 200:
                continue

            weather_data = weather_response.json()
            pollution_data = pollution_response.json()
            
            if "main" in weather_data and "list" in pollution_data:
                key_usage[api_key] += 2 
                timezone_offset = weather_data["timezone"]  

                #pollutant data
                pollutants = pollution_data["list"][0].get("components", {})

                # Local Time
                last_updated = datetime.utcnow() + timedelta(seconds=timezone_offset)
                last_updated_str = last_updated.strftime('%Y-%m-%d %H:%M:%S')

                return {
                    "Latitude": lat, "Longitude": lon, "City": city,
                    "Weather": weather_data["weather"][0]["description"].title(),
                    "Weather Icon": f"http://openweathermap.org/img/wn/{weather_data['weather'][0]['icon']}@2x.png",
                    "Temperature (°C)": f"{round(weather_data['main'].get('temp', 0), 2)}°C",
                    "Pressure (hPa)": f"{weather_data['main'].get('pressure', 0)} hPa",
                    "Humidity (%)": f"{weather_data['main'].get('humidity', 0)}%",
                    "Visibility (km)": f"{round(weather_data.get('visibility', 0) / 1000, 2)} km",
                    "Wind Speed (km/h)": f"{round(weather_data.get('wind', {}).get('speed', 0) * 3.6, 2)} km/h",
                    "Wind Degree (°)": f"{weather_data.get('wind', {}).get('deg', 0)}°",
                    "Cloud Coverage (%)": f"{weather_data.get('clouds', {}).get('all', 0)}%",
                    "Sunrise": datetime.utcfromtimestamp(weather_data["sys"]["sunrise"] + timezone_offset).strftime('%I:%M %p'),
                    "Sunset": datetime.utcfromtimestamp(weather_data["sys"]["sunset"] + timezone_offset).strftime('%I:%M %p'),
                    "AQI": pollution_data["list"][0]["main"]["aqi"],
                    "CO": str(pollutants.get("co", "0")), "NO": str(pollutants.get("no", "0")),
                    "NO₂": str(pollutants.get("no2", "0")), "O₃": str(pollutants.get("o3", "0")),
                    "SO₂": str(pollutants.get("so2", "0")), "PM2.5": str(pollutants.get("pm2_5", "0")),
                    "PM10": str(pollutants.get("pm10", "0")), "NH₃": str(pollutants.get("nh3", "0")),
                    "Last Updated": last_updated_str
                }
        except Exception as e:
    return None

# Multithreading
def fetch_all_data():
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(lambda loc: fetch_data(*loc), districts))

    data_df = pd.DataFrame(filter(None, results))

    if data_df.empty:
        return

    # Update Google Sheet
    worksheet.clear()
    set_with_dataframe(worksheet, data_df, include_index=False, include_column_header=True)

if __name__ == "__main__":
    fetch_all_data()

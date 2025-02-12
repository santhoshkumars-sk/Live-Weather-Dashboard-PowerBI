import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from datetime import datetime

# Google Service Account JSON (hardcoded)
google_creds_json ={
  "type": "service_account",
  "project_id": "weatherandnpollution",
  "private_key_id": "710b0302a61415435b129b5874c99bfb1602b273",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDM4rMzaCgN+X4H\n9ychrEuVOepHsi1z87glCHRyYxwGSI6kUbt7cxSKSovRdubgEa5Ji6ylA5JlnjSN\nqxjktlN1Nh1gzP0kWzSqJ2yNkOIm3eJJgozB83tFEi3oLfTS444+6tl/AdrLGlX7\nNwcWN7AwAIICVvLw6HGbpIh8esjSf8j2H/ZJFGelQhe0rNW4tY8nAAO0w8wMvpbL\ndGKB5zRgvz+jxuPqHtLZoNI0QoQemy2T+X9nTmgPsmFtRX2Wfln9qzS4bM0+DfUk\nHX2/q6sbx+5IB3nLzOm7RHXVakrn5+AUQ4FIL5XOSFWqtuqilOzSaSAoxlehWvph\nl6kgoZ5FAgMBAAECggEAERFKgHwK/X0HkBt5OEoizWZ9cHsJ48iip1JwvDhVijrK\n8n/M2IAR7uI+YFThbzrcfrZtYPKu+ZryrEww6Zk8UxLkDXS4xWmj9bpihdl9qWNe\n+XowBmvQHJp4K15WsHxcf2Zvd1p8yDdkVbt0B5hcgOct7WAiYp9ZGsWGSX5Yp9uZ\n6/4O6U8wC5m70GqvLdilBIak9w6bONq+BdjkrEBvslwSC0bUOUXAsKpTyXpjFGiH\n78fsKuGj42sckkWxgr7UTI+fkVqeArB+qcjWqbKAzBMWdpL51BlYGLiTlPcMYv0J\nKq/FThoor9g0M8gp0ceNTb9KIux+9WKPE2yIk/yAKQKBgQDsm6Kr7CiZTiTFwrqt\ndBU2VIN0H2ISbniZEicguyJmpYRKIzO+1dxtW7rK2EZKgqM5oui4YP3CpTr06oJ9\nTEYFXjKiFM4CFQ3M0u4d2IbeRCsYVwnzCO3gam4xYjrAkDvW+6lrH62Gj1pXePep\nREO45GSQkbAXlyL0DdOT1mmoXQKBgQDdrXv8ZoF6iCBgPm0MKLv073c4Nh1PHyz/\n8218bkyLi7t8NILVm+bdt68C/O2YmfVxk5wSKGsEjE93CxoSt5wUDPxX7p5PvjuA\nar+mfw505seUVCcqq7LiAIX5mfZ1/mbyBtw+M6F9mc13tdW8LAk6dwO/6d4ZaC1B\n1FFR9G5PCQKBgADGte7odG2kUvyPGfutsNAf0NqVq8WxtRWTeKtj0cYSbu+rU+Uh\nYHq/XUqDER/3c7+hqC/Kqap0zk+1YT41/NjRqUrc1JwvI39zhbJ1XmPmR1nqFy9D\n7TvaPYhN8TFmh7u50aF3ViNl3v8Ad5eHkM1PeoD66V/6b/PrA7Gtd2VFAoGAIw5Y\nWSXsu+/jDtVQcWfPsYBdHiqQn/6SIzffzJm0ySyjzZSQTC+HZvCxYIuXF1bhlcB+\nIfD6W5HEz1KWIPegWxtinJVDu+CdkEVRYYceRiZo0XAtcXYNp+flR/+tFiPRK90h\n30RS+YXlFFVRI5zjKLfBhAv775Cl948X/Rnmh+ECgYEAj6jWMMOqdLRf6gF51bDj\n8h2pXgMYBQj3VwANJjUiWszGm2JhJea4PCpehSbnFzgb9y6DFI0/N/G4aSkaT6dN\ntQU1AEi7FxkQNnGe+rkceXoa0XEaeVew84a1ZAWxuvRPK0WxppH8oXkfmnTic7Ip\nDdkPacO4XxQCSG1yEodndwg=\n-----END PRIVATE KEY-----\n",
  "client_email": "sheets-api-access@weatherandnpollution.iam.gserviceaccount.com",
  "client_id": "115010347098603102865",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sheets-api-access%40weatherandnpollution.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


creds_dict = json.loads(google_creds_json)

# Google Sheets API
creds = Credentials.from_service_account_info(creds_dict)
gc = gspread.authorize(creds)

# Google Sheet URL (hardcoded)
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1mWIDMWhis5IzvtyXlACPueuXhjTdQzECiarvYB-nhNQ/edit?usp=sharing"
spreadsheet = gc.open_by_url(SPREADSHEET_URL)
worksheet = spreadsheet.sheet1

# CSV containing list of city names (URL)
CSV_URL = "https://raw.githubusercontent.com/santhoshkumars-sk/weather-and-pollution-dashboard/main/latnlon.csv"
districts_df = pd.read_csv(CSV_URL)
districts = districts_df[["Latitude", "Longitude", "City"]].values.tolist()

# OpenWeather API keys (hardcoded)
api_keys =[
    "a9496eb37d64660642d2c78a1b5c48b9", "df6e7472c74227efcecc770beb4107b5", "256156d28a4575e841a3cce2fdfc060b",
    "bb3abfbd0870985604b23cf158cf8a33", "22b7f3537fa4da3978cdd4360f888378", "a889e681068ddf3d6bcf127302613457",
    "1a73f578941760b7b002aa8e25010b93", "608db310c58c773ff27a083f2ff05a55", "07a320e2e4fe07886409e42bb48b0bac",
    "87d845b0b6cf29baa1a73cc34b067a95", "2163dae3061ac351ccb1faf00b2b44ed", "e7e1e0d45278fb01e3aaf52d903129a1",
    "86b83cb898657f25b3b9552648cd1b94", "16f6961f3ba1c07126321f28f28db04a", "5f69dafbf1f84299660d0edfb64b10dd",
    "1f2a35cc30d17d6c14bb66c12b9dc65c", "cc799d032932cfbadd643291b3d9c497", "b71891f84860875c07a5ec9704fb654f",
    "45be5e394e7a2cf41bf01ef87aae082b", "4c97be248ca496141da169fd841ecb78", "7eb10c175dacd870744b091b68544ced",
    "0f84b67b5728e6e8ca9fdefae37162c7", "7e87948002156bfcf9993502016e2660", "d6477696b63c2e661af64eead58c11d9",
    "969ef6a03b3068fa49586c69b31c06d3", "7306a7e1bd42e6d28d009ded2ce05f9f", "8a5d6ad154cf268ec102839dcdb29f37",
    "da5d8e32fc9254d12759e8d19b828aef"
]
api_key_cycle = cycle(api_keys)
key_usage = {key: 0 for key in api_keys}

# Column names
HEADERS = ["Latitude", "Longitude", "City", "Weather", "Weather Icon", "Temperature (°C)",
           "Pressure (hPa)", "Humidity (%)", "Visibility (km)", "Wind Speed (km/h)", "Wind Degree (°)",
           "Cloud Coverage (%)", "Sunrise", "Sunset", "AQI", "CO", "NO", "NO₂", "O₃", "SO₂", "PM2.5", "PM10", "NH₃", "Last Updated"]

# Fetch Weather & Pollution Data
def fetch_data(lat, lon, city):
    for _ in range(3):  # Retries
        api_key = next(api_key_cycle)
        if key_usage[api_key] >= 55:
            continue
        
        weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        pollution_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"

        weather_data = requests.get(weather_url).json()
        pollution_data = requests.get(pollution_url).json()
        
        if "main" in weather_data and "list" in pollution_data:
            key_usage[api_key] += 2
            timezone_offset = weather_data["timezone"] 
            pollutants = pollution_data["list"][0]["components"]
            
            return {
                "Latitude": lat, "Longitude": lon, "City": city,
                "Weather": weather_data["weather"][0]["description"].title(),
                "Weather Icon": f"http://openweathermap.org/img/wn/{weather_data['weather'][0]['icon']}@2x.png",
                "Temperature (°C)": f"{round(weather_data['main'].get('temp'), 2)}°C",
                "Pressure (hPa)": f"{weather_data['main'].get('pressure')} hPa",
                "Humidity (%)": f"{weather_data['main'].get('humidity')}%",
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
                "Last Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    return None

# Multithreading
def fetch_all_data():
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(lambda loc: fetch_data(*loc), districts))
    
    data_df = pd.DataFrame(filter(None, results))
    worksheet.clear()
    set_with_dataframe(worksheet, data_df, include_index=False, include_column_header=True)

if __name__ == "__main__":
    fetch_all_data()

# Live Weather and Pollution Dashboard - Power BI

This repository hosts a real-time **Live Weather and Pollution Dashboard** built using **Power BI**, with automated data retrieval and processing using **Python** scripts and **GitHub Actions**.

## Features

- **Real-Time Data**: Retrieves weather and pollution data every 2 hours using APIs.
- **Power BI Visualization**: Displays trends, forecasts, and real-time insights.
- **Automated Updates**: GitHub Actions run the scripts periodically and store the results in **Google Sheets**.
- **Temperature Trends**: Line chart visualization of temperature changes over time.

## Live Data

You can view the latest **weather and pollution data** in this **[Google Sheet](https://docs.google.com/spreadsheets/d/e/2PACX-1vQht3E9SJCypJKu311H2glWNFIXXKWVg-cNHs5RuuffN5E4iglOJb7KHC01YP396ZcHvJY35yeBsHD2/pubhtml)** (view-only access).

## Dashboard Preview

![Screenshot (194)](https://github.com/user-attachments/assets/04a89222-e371-469b-a749-ec8372121794)

## Repository Structure

```
Live-Weather-Dashboard-PowerBI-main/
├── Dashboard.pbix               # Power BI dashboard file
├── city_coordinates.csv         # List of city coordinates
├── requirements.txt             # Dependencies
├── script.py                    # Main data retrieval script
├── temperature_forecast.py      # Temperature forecasting script
└── .github/workflows/           # GitHub Actions workflows
    ├── run_script.yml           # Automates script.py execution
    └── run_temperature.yml      # Automates temperature_forecast.py execution
```

## API Key Configuration
Before running the scripts, make sure to replace `YOUR_API_KEY` in `script.py` with your actual API key for fetching weather and pollution data.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/santhoshkumars-sk/Live-Weather-Dashboard-PowerBI.git
cd Live-Weather-Dashboard-PowerBI-main
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Running the Scripts Manually

To fetch the latest weather and pollution data:

```bash
python script.py
```

To generate temperature forecasts:

```bash
python temperature_forecast.py
```

## GitHub Actions Automation

- The **run\_script.yml** workflow executes `script.py` every **2 hours** to fetch and update real-time data in Google Sheets.
- The **run\_temperature.yml** workflow executes `temperature_forecast.py` periodically to update temperature trends.





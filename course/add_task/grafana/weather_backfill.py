import requests
import csv
import os
import time
from influxdb_client import InfluxDBClient, Point, WriteOptions
from datetime import datetime, timedelta, timezone


CITY = "Almaty"
LAT = 43.25
LON = 76.95

START_DATE = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
END_DATE = datetime.utcnow().strftime("%Y-%m-%d")

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "TOKEN"
INFLUX_ORG = "weather_org"
INFLUX_BUCKET = "weather_bucket"

CSV_FILE = "weather_archive.csv"




url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": LAT,
    "longitude": LON,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "daily": "temperature_2m_mean,windspeed_10m_max",
    "timezone": "UTC"
}

data = requests.get(url, params=params).json()["daily"]

dates = data["time"]
temps = data["temperature_2m_mean"]
winds = data["windspeed_10m_max"]



file_exists = os.path.isfile(CSV_FILE)

with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["timestamp", "temperature", "windspeed"]
    )

    if not file_exists:
        writer.writeheader()

    for date, temp, wind in zip(dates, temps, winds):
        writer.writerow({
            "timestamp": datetime.fromisoformat(date).replace(tzinfo=timezone.utc).isoformat(),
            "temperature": temp,
            "windspeed": wind
        })



client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(
    write_options=WriteOptions(batch_size=1)
)

for date, temp, wind in zip(dates, temps, winds):
    point = (
        Point("weather")
        .tag("city", CITY)
        .tag("source", "open-meteo-archive")
        .field("temperature", temp)
        .field("windspeed", wind)
        .time(datetime.fromisoformat(date).replace(tzinfo=timezone.utc))
    )
    write_api.write(bucket=INFLUX_BUCKET, record=point)
    time.sleep(0.05)

write_api.flush()
client.close()

print("Backfill completed for last 3 months")

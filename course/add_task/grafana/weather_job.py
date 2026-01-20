import requests
import csv
import os
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WriteOptions


INFLUX_URL = "http://localhost:8086"
TOKEN = "TOKEN"   
ORG = "weather_org"
BUCKET = "weather"

CSV_FILE = "weather_data.csv"



def get_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=51.16&longitude=71.43"
        "&current_weather=true"
    )
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()["current_weather"]

    return {
        "timestamp": datetime.now(timezone.utc),
        "temperature": data["temperature"],
        "windspeed": data["windspeed"]
    }


def write_csv(row):
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "temperature", "windspeed"]
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "timestamp": row["timestamp"].isoformat(),
            "temperature": row["temperature"],
            "windspeed": row["windspeed"]
        })


def write_influx(row):
    client = InfluxDBClient(
        url=INFLUX_URL,
        token=TOKEN,
        org=ORG
    )

    write_api = client.write_api(
        write_options=WriteOptions(batch_size=1)
    )

    point = (
        Point("weather")
        .field("temperature", row["temperature"])
        .field("windspeed", row["windspeed"])
        .time(row["timestamp"])
    )

    write_api.write(
        bucket=BUCKET,
        org=ORG,
        record=point
    )

    write_api.flush()   
    client.close()      


if __name__ == "__main__":
    weather = get_weather()
    write_csv(weather)
    write_influx(weather)
    print("Weather ingested:", weather)

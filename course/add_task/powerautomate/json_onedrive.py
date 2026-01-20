import pandas as pd
import json
import os


CSV_FILE = path
ONEDRIVE_FOLDER = folder
OUTPUT_FILE = "weather_archive.json"
DAYS = 90



df = pd.read_csv(CSV_FILE)


df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")


df = df.dropna(subset=["timestamp"])


cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=DAYS)
df = df[df["timestamp"] >= cutoff]


df = df.sort_values("timestamp")


data = df.to_dict(orient="records")


os.makedirs(ONEDRIVE_FOLDER, exist_ok=True)


output_path = os.path.join(ONEDRIVE_FOLDER, OUTPUT_FILE)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, default=str)

print(f" JSON updated in OneDrive: {output_path}")
print(f" Rows exported: {len(data)}")

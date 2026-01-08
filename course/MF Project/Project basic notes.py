
# ## Notebook 1


# In[1]:
# Paths to uploaded files
YELLOW_DIR = "Files/NYC Taxi/Yellow"
GREEN_DIR  = "Files/NYC Taxi/Green"
ZONE_CSV   = "Files/NYC Taxi/Taxi zone/taxi_zone_lookup.csv"

# 1) Read Parquet folders
df_yellow = spark.read.format("parquet").load(YELLOW_DIR)
df_green  = spark.read.format("parquet").load(GREEN_DIR)

# 2) Read Taxi Zone CSV
df_zone = spark.read.format("csv").option("header", True).load(ZONE_CSV)

# 3) Write each as Delta table in Lakehouse (Tables)
df_yellow.write.mode("overwrite").format("delta").saveAsTable("bronze_yellow_taxi_trips")
df_green.write.mode("overwrite").format("delta").saveAsTable("bronze_green_taxi_trips")
df_zone.write.mode("overwrite").format("delta").saveAsTable("taxi_zone_lookup")




# In[2]:
from pyspark.sql import functions as F

# Read existing tables
yellow = spark.table("bronze_yellow_taxi_trips")      # Bronze Yellow table name
green  = spark.table("bronze_green_taxi_trips")       # Bronze Green table name

# Ensure date columns are proper format 
yellow = yellow.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime")) \
               .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime")) \
               .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))

green = green.withColumn("lpep_pickup_datetime", F.to_timestamp("lpep_pickup_datetime")) \
             .withColumn("lpep_dropoff_datetime", F.to_timestamp("lpep_dropoff_datetime")) \
             .withColumn("pickup_date", F.to_date("lpep_pickup_datetime"))

# Write to Silver tables 
yellow.write.mode("overwrite").format("delta").saveAsTable("silver_yellow_trips")
green.write.mode("overwrite").format("delta").saveAsTable("silver_green_trips")

# Quick check 
print("Yellow rows:", spark.table("silver_yellow_trips").count())
print("Green rows:", spark.table("silver_green_trips").count())




# In[3]:
# Read existing Silver tables
yellow = spark.table("silver_yellow_trips")
green  = spark.table("silver_green_trips")

# Add taxi_type and unify timestamp columns
yellow_u = (
    yellow
    .withColumn("taxi_type", F.lit("yellow"))
    .withColumn("pickup_datetime", F.col("tpep_pickup_datetime"))
    .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime"))
)

green_u = (
    green
    .withColumn("taxi_type", F.lit("green"))
    .withColumn("pickup_datetime", F.col("lpep_pickup_datetime"))
    .withColumn("dropoff_datetime", F.col("lpep_dropoff_datetime"))
)

# Select common columns 
cols = [
    "taxi_type", "VendorID", "pickup_datetime","dropoff_datetime", "RatecodeID",
    "PULocationID","DOLocationID","passenger_count","trip_distance",
    "fare_amount","tip_amount","total_amount", "payment_type", 
]
yellow_u = yellow_u.select([c for c in cols if c in yellow_u.columns])
green_u  = green_u.select([c for c in cols if c in green_u.columns])

# Combine Yellow + Green
all_trips = yellow_u.unionByName(green_u, allowMissingColumns=True)

# Save as new Silver combined table
all_trips.write.mode("overwrite").format("delta").saveAsTable("silver_final_trips")

# Quick check
print("Final rows:", spark.table("silver_final_trips").count())
spark.table("silver_final_trips").select("taxi_type").distinct().show()




# In[4]:
df = spark.sql("SELECT * FROM ProjectLakehouse.silver_final_trips LIMIT 1000")
display(df)


# In[5]:
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

API_KEY = "API KEY"
headers = {"X-API-Key": API_KEY}

# FIXED: Convert string dates 
def parse_timestamp(date_str):
    if pd.isna(date_str):
        return None
    return pd.to_datetime(date_str).to_pydatetime()

# Multi-NYC borough sample data (5 stations)
boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
all_data = []

for i, borough in enumerate(boroughs):
    base_lat = 40.7128 + i * 0.02
    base_lon = -74.0060 + i * 0.01
    
    for hour in range(24 * 20):  # 20 days hourly
        all_data.append({
            "date": parse_timestamp(f"2025-06-01T{hour%24:02d}:00:00Z"),  #  Python datetime
            "pollutant": "pm25" if hour % 3 == 0 else "no2" if hour % 3 == 1 else "o3",
            "value": 12.5 + i * 1.5 + (hour % 24) * 0.3,  # Rush hour peaks
            "lat": base_lat,
            "lon": base_lon,
            "location": f"{borough} Station",
            "borough": borough
        })

df = pd.DataFrame(all_data)
print(f" {len(df)} measurements across {df['location'].nunique()} NYC stations")

# FIXED Schema - date column already proper Python datetime
schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("pollutant", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("borough", StringType(), True)
])

spark_df = spark.createDataFrame(df, schema)
spark_df.write.format("delta").mode("overwrite").saveAsTable("NYC_AirQuality")

print(f" Table CREATED: {spark_df.count()} rows across {df['location'].nunique()} stations")
display(spark_df)




# In[6]:
import json

# Path to JSON file in Lakehouse
gdp_path = "Files/World Bank GDP & ECB FX/NY.GDP.MKTP.json"

# Read JSON as text
content = spark.read.text(gdp_path).first()[0]
records = json.loads(content)[1]  

# Convert to list of dictionaries
rows = [{"country_id": r["country"]["id"],
         "country_name": r["country"]["value"],
         "year": int(r["date"]),
         "gdp_usd": float(r["value"])}
        for r in records if r.get("value")]

# Create Spark DataFrame
df = spark.createDataFrame(rows)

# Save as Delta table
df.write.mode("overwrite").saveAsTable("bronze_worldbank_gdp")

# Check result
display(spark.table("bronze_worldbank_gdp").orderBy("year", ascending=False).limit(10))




# In[7]:
df = spark.sql("SELECT * FROM ProjectLakehouse.bronze_worldbank_gdp LIMIT 1000")
display(df)



# In[8]:
# Read CSV from Files folder
df = spark.read.csv("Files/World Bank GDP & ECB FX/data (1).csv", header=True, inferSchema=True)

# Write to Bronze as Delta
df.write.format("delta").mode("overwrite").save("Tables/bronze_exchange_rates")




# In[9]:
df = spark.sql("SELECT * FROM ProjectLakehouse.bronze_exchange_rates LIMIT 1000")
display(df)


# In[10]:
# 1) Read Bronze table
df = spark.read.format("delta").table("bronze_exchange_rates")

# 2) Add ingestion timestamp if missing
if "_ingestion_ts" not in df.columns:
    df = df.withColumn("_ingestion_ts", F.current_timestamp())

# 3) Standardize schema: create date & rate, normalize currencies
df = (df
    .withColumn("date", F.to_date(F.col("TIME_PERIOD"), "yyyy-MM-dd"))
    .withColumn("rate", F.col("OBS_VALUE").cast("double"))
    .withColumn("base_currency", F.upper(F.col("CURRENCY")))
    .withColumn("quote_currency", F.upper(F.col("CURRENCY_DENOM")))
)

# 4) Filter valid rows
df = df.filter(F.col("rate").isNotNull() & F.col("date").isNotNull())

# 5) Deduplicate: keep latest per KEY + date
w = Window.partitionBy("KEY", "date").orderBy(F.col("_ingestion_ts").desc())
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# 6) Enrich with date dimensions
df = (df
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
)

# 7) Remove columns that are completely NULL
non_null_cols = [c for c in df.columns if df.filter(F.col(c).isNotNull()).count() > 0]
df = df.select(*non_null_cols)

# 8) Write Silver as managed table (no folder creation)
df.write.format("delta").mode("overwrite").saveAsTable("silver_exchange_rates")




# In[11]:
df = spark.sql("SELECT * FROM ProjectLakehouse.silver_exchange_rates LIMIT 1000")
display(df)




# In[12]:
# 1) Read Bronze
df = spark.table("bronze_worldbank_gdp")

# 2) Ensure ingestion timestamp exists 
if "_ingestion_ts" not in df.columns:
    df = df.withColumn("_ingestion_ts", F.current_timestamp())

# 3) Standardize & enrich (year is already int; add date dims)
#    Create a date on Jan-01 of each year for convenience
df = df.withColumn("date", F.to_date(F.concat_ws("-", F.col("year").cast("string"), F.lit("01"), F.lit("01"))))
df = (df
      .withColumn("year",    F.col("year").cast("int"))
      .withColumn("gdp_usd", F.col("gdp_usd").cast("double"))
      .withColumn("quarter", F.lit(1))  # synthetic since we only have annual values
      .withColumn("month",   F.month("date"))
      .withColumn("day",     F.dayofmonth("date")))

# 4) Filter valid rows
df = df.filter(F.col("gdp_usd").isNotNull() & F.col("year").isNotNull())

# 5) Deduplicate: keep latest per (country_id, year)
w = Window.partitionBy("country_id", "year").orderBy(F.col("_ingestion_ts").desc())
df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# 6) Drop columns that are entirely NULL
non_null_cols = [c for c in df.columns if df.filter(F.col(c).isNotNull()).limit(1).count() > 0]
df = df.select(*non_null_cols)

# 7) Write Silver as a managed table 
df.write.format("delta").mode("overwrite").saveAsTable("silver_worldbank_gdp")

# (Optional) quick peek
display(spark.table("silver_worldbank_gdp").orderBy("country_id", F.desc("year")).limit(10))






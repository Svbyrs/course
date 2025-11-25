
import json
import psycopg2

# Load JSON
with open(r"C:\Users\SanzharSabyr\Desktop\FP\python\course\task1_d_clean.json", "r", encoding="utf-8") as f:
    books = json.load(f)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="course_db",
    user="postgres",
    password="1128327",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Create table with NUMERIC id
cur.execute("""
CREATE TABLE IF NOT EXISTS books (
    id NUMERIC PRIMARY KEY,
    title TEXT,
    author TEXT,
    genre TEXT,
    publisher TEXT,
    year INT,
    price TEXT
)
""")

# Insert data
data_list = [(b["id"], b["title"], b["author"], b["genre"], b["publisher"], b["year"], b["price"]) for b in books]
cur.executemany("""
INSERT INTO books (id, title, author, genre, publisher, year, price)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""", data_list)

conn.commit()
cur.close()
conn.close()

print(f" Inserted {len(data_list)} records into PostgreSQL")

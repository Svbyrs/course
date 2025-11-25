
import re
import json

# Step 1: Read the invalid JSON file
with open("task1_d.json", "r", encoding="utf-8") as f:
    raw_data = f.read()

# Step 2: Extract all records using regex
pattern = re.compile(r'\{:id=>\s*(\d+),\s*:title=>"(.*?)",\s*:author=>"(.*?)",\s*:genre=>"(.*?)",\s*:publisher=>"(.*?)",\s*:year=>(\d+),\s*:price=>"(.*?)"\}')
matches = pattern.findall(raw_data)

# Step 3: Convert matches to list of dictionaries
books = []
for m in matches:
    book_id, title, author, genre, publisher, year, price = m
    books.append({
        "id": int(book_id),
        "title": title,
        "author": author,
        "genre": genre,
        "publisher": publisher,
        "year": int(year),
        "price": price
    })

# Step 4: Save as valid JSON
with open("task1_d_clean.json", "w", encoding="utf-8") as f:
    json.dump(books, f, indent=4, ensure_ascii=False)

print(f"Converted {len(books)} records to valid JSON and saved as task1_d_clean.json")

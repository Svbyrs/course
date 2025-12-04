
import pandas as pd
import numpy as np
import yaml
import os
import re
import matplotlib.pyplot as plt

folder_path = r"C:\Users\SanzharSabyr\Desktop\FP\python\course\Task4\data\DATA1"

users_df = pd.read_csv(os.path.join(folder_path, "users.csv"))
with open(os.path.join(folder_path, "books.yaml"), "r", encoding="utf-8") as f:
    books_data = yaml.safe_load(f)
books_df = pd.DataFrame(books_data)
orders_df = pd.read_parquet(os.path.join(folder_path, "orders.parquet"))


def normalize(df):
    df.columns = [re.sub(r'[^0-9a-zA-Z]+', '_', col).strip('_').lower() for col in df.columns]
    return df

users_df = normalize(users_df)
books_df = normalize(books_df)
orders_df = normalize(orders_df)

print("Users columns:", users_df.columns)
print("Books columns:", books_df.columns)
print("Orders columns:", orders_df.columns)


user_key = [c for c in orders_df.columns if 'user' in c][0]
book_key = [c for c in orders_df.columns if 'book' in c][0]


merged_df = (
    orders_df
    .merge(users_df, left_on=user_key, right_on="id", how="left", suffixes=("", "_user"))
    .merge(books_df, left_on=book_key, right_on="id", how="left", suffixes=("", "_book"))
)

EURO_TO_USD = 1.2

def normalize_number_token(s: str) -> str:
    s = s.replace("\u00A0", " ").strip()
    s = s.replace("¢", ".")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    elif "," in s and "." in s:
        s = s.replace(",", "")
    s = re.sub(r"\s+", " ", s)
    return s

def to_usd(value):
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)

    s = normalize_number_token(str(value))
    s_lower = s.lower()

    has_usd = bool(re.search(r"(\$|usd|us\$)", s_lower))
    has_eur = bool(re.search(r"(€|eur|euro)", s_lower))

    s_num = re.sub(r"(usd|us\$|eur|euro|\$|€)", " ", s_lower, flags=re.IGNORECASE)
    s_num = normalize_number_token(s_num)

    m = re.search(r"[-+]?\d+(?:\.\d+)?", s_num)
    if not m:
        return np.nan
    val = float(m.group(0))

    return val * EURO_TO_USD if has_eur and not has_usd else val

merged_df["quantity"] = pd.to_numeric(merged_df["quantity"], errors="coerce").fillna(0)
merged_df["unit_price_usd"] = merged_df["unit_price"].apply(to_usd)
merged_df["unit_price_usd"] = pd.to_numeric(merged_df["unit_price_usd"], errors="coerce").fillna(0)

merged_df["paid_price"] = (merged_df["quantity"] * merged_df["unit_price_usd"]).round(2)

print("Added paid_price (USD)")
print(merged_df[["quantity", "unit_price", "unit_price_usd", "paid_price"]].head())


if "timestamp" in merged_df.columns:
    merged_df["timestamp"] = pd.to_datetime(
        merged_df["timestamp"], errors="coerce", dayfirst=False, utc=True
    )

    valid_count = merged_df["timestamp"].notna().sum()
    print(f"Parsed {valid_count} valid timestamps out of {len(merged_df)} rows.")

    
    merged_df["year_extracted"] = merged_df["timestamp"].dt.year
    merged_df["month_extracted"] = merged_df["timestamp"].dt.month
    merged_df["day_extracted"] = merged_df["timestamp"].dt.day
    merged_df["date"] = merged_df["timestamp"].dt.date
else:
    print("No timestamp column found.")


cols_to_show = [c for c in ["timestamp", "year_extracted", "month_extracted", "day_extracted", "date"] if c in merged_df.columns]
print("Extracted date parts:")
print(merged_df[cols_to_show].head())


if "date" in merged_df.columns and merged_df["date"].notna().any():
    daily_revenue = (
        merged_df.groupby("date", dropna=True)["paid_price"]
        .sum()
        .reset_index()
        .rename(columns={"paid_price": "total_revenue"})
    )

    top_5_days = daily_revenue.sort_values(by="total_revenue", ascending=False).head(5)

    print("\nTop 5 days by revenue:")
    print(top_5_days)
else:
    print("Cannot compute daily revenue because 'date' column was not created or has no valid values.")



if "author" in books_df.columns:
    author_sets = books_df["author"].dropna().apply(
        lambda x: frozenset([a.strip() for a in str(x).split(",") if a.strip()])
    )
    unique_author_sets = set(author_sets)
    print(f"Number of unique author sets: {len(unique_author_sets)}")

    print("Examples of unique author sets:")
    for s in list(unique_author_sets)[:5]:
        print(s)
else:
    print("No 'author' column found in books_df.")



TOP_N = 10  

if {"author", "quantity"}.issubset(merged_df.columns):
    merged_df["quantity"] = pd.to_numeric(merged_df["quantity"], errors="coerce").fillna(0)

    def to_author_set(x):
        if pd.isna(x):
            return np.nan
        parts = [a.strip() for a in str(x).split(",") if a.strip()]
        return frozenset(parts) if parts else np.nan

    merged_df["author_set"] = merged_df["author"].apply(to_author_set)

    author_set_sales = (
        merged_df.dropna(subset=["author_set"])
                 .groupby("author_set", dropna=True)["quantity"]
                 .sum()
                 .reset_index()
                 .sort_values("quantity", ascending=False)
                 .head(TOP_N)
    )

    print(f"Top {TOP_N} author sets by sold book count:")
    for _, row in author_set_sales.iterrows():
        print(f"- {list(row['author_set'])} — {int(row['quantity'])} books sold")
else:
    print("Missing 'author' or 'quantity' column in merged_df.")



uid_col = "user_id"
merged_df["paid_price"] = pd.to_numeric(merged_df["paid_price"], errors="coerce").fillna(0)


top_id = merged_df.groupby(uid_col)["paid_price"].sum().idxmax()
top_total = merged_df.groupby(uid_col)["paid_price"].sum().max()


top_rows = merged_df[merged_df[uid_col] == top_id]


def collect_values(df, keyword):
    return sorted({v for v in df.filter(like=keyword).values.flatten() if pd.notna(v)})

emails = collect_values(top_rows, "email")
phones = collect_values(top_rows, "phone")
addresses = collect_values(top_rows, "address")
names = collect_values(top_rows, "name")

print(f" Top customer: user_id={top_id}, total spending=${top_total:,.2f}")
print(f"- emails: {emails}")
print(f"- phones: {phones}")
print(f"- addresses: {addresses}")
print(f"- names: {names}")



plt.figure(figsize=(20, 5))
plt.plot(daily_revenue["date"], daily_revenue["total_revenue"], marker="o", color="blue")
plt.title("Daily Revenue")
plt.xlabel("Date")
plt.ylabel("Total Revenue (USD)")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

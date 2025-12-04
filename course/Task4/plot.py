
import os
import re
import yaml
import argparse
from datetime import datetime
from typing import List, Dict, Set

import numpy as np
import pandas as pd
import plotly.express as px


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()
    df.columns = [re.sub(r'[^0-9a-zA-Z]+', '_', col).strip('_').lower() for col in df.columns]
    return df

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


def normalize_authors(val) -> List[str]:
    
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        items = [str(x).strip() for x in val if pd.notna(x)]
    else:
        items = [s.strip() for s in str(val).split(",") if s.strip()]
    return sorted(set(items))


def collect_values(df: pd.DataFrame, keyword: str) -> Set[str]:
    
    vals = set()
    for col in df.columns:
        if keyword in col:
            for v in df[col].dropna().astype(str).tolist():
                vv = v.strip()
                if vv:
                    vals.add(vv)
    return vals

def best_display_name(row: pd.Series) -> str:
    
    candidates = []
    for col in ["name", "full_name", "fullname", "display_name"]:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            candidates.append(str(row[col]).strip())

    first = str(row.get("first_name", "") or "").strip()
    last  = str(row.get("last_name", "") or "").strip()
    if first or last:
        candidates.append((first + " " + last).strip())

    
    for col in [c for c in row.index if "email" in c]:
        val = str(row[col]).strip()
        if val:
            candidates.append(val)
            break

    for c in candidates:
        if c:
            return c
    return ""


def build_dashboard(folder_path: str, output_html: str = "dashboard.html") -> str:
    
    users_df = pd.read_csv(os.path.join(folder_path, "users.csv"))
    with open(os.path.join(folder_path, "books.yaml"), "r", encoding="utf-8") as f:
        books_data = yaml.safe_load(f)
    books_df = pd.DataFrame(books_data)
    orders_df = pd.read_parquet(os.path.join(folder_path, "orders.parquet"))

    
    users_df = normalize(users_df)
    books_df = normalize(books_df)
    orders_df = normalize(orders_df)

    
    user_key_candidates = [c for c in orders_df.columns if "user" in c]
    book_key_candidates = [c for c in orders_df.columns if "book" in c]
    if not user_key_candidates:
        raise ValueError("Could not find a user key in orders columns.")
    if not book_key_candidates:
        raise ValueError("Could not find a book key in orders columns.")
    user_key = user_key_candidates[0]
    book_key = book_key_candidates[0]

    
    merged_df = (
        orders_df
        .merge(users_df, left_on=user_key, right_on="id", how="left", suffixes=("", "_user"))
        .merge(books_df, left_on=book_key, right_on="id", how="left", suffixes=("", "_book"))
    )

    
    merged_df["quantity"] = pd.to_numeric(merged_df["quantity"], errors="coerce").fillna(0)
    merged_df["unit_price_usd"] = pd.to_numeric(merged_df["unit_price"].apply(to_usd), errors="coerce").fillna(0)
    merged_df["paid_price"] = (merged_df["quantity"] * merged_df["unit_price_usd"]).round(2)

    
    if "timestamp" not in merged_df.columns:
        raise ValueError("No 'timestamp' column found after merge.")
    merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"], errors="coerce", dayfirst=False, utc=True)
    merged_df["date"] = merged_df["timestamp"].dt.date

    
    daily_revenue = (
        merged_df.dropna(subset=["date"])
                 .groupby("date", dropna=True)["paid_price"]
                 .sum()
                 .reset_index()
                 .rename(columns={"paid_price": "total_revenue"})
    )
    daily_revenue_sorted = daily_revenue.sort_values("total_revenue", ascending=False)
    top5_pairs = [
        {"date": pd.to_datetime(str(d)).strftime("%Y-%m-%d"), "revenue": float(r)}
        for d, r in daily_revenue_sorted.head(5).to_records(index=False)
    ]

    
    unique_users = int(merged_df[user_key].nunique())

    
    if "author" in merged_df.columns:
        merged_df["_authors_list"] = merged_df["author"].apply(normalize_authors)
        merged_df["_authors_key"] = merged_df["_authors_list"].apply(lambda xs: "|".join(xs))
        
        non_empty_keys = merged_df["_authors_key"][merged_df["_authors_key"].str.len() > 0]
        unique_author_sets = int(pd.Series(non_empty_keys).nunique())
    else:
        unique_author_sets = 0

    
    if "author" in merged_df.columns:
        exploded = merged_df[["paid_price", "_authors_list"]].explode("_authors_list")
        exploded = exploded.dropna(subset=["_authors_list"])
        by_author = exploded.groupby("_authors_list", as_index=False)["paid_price"].sum()
        if not by_author.empty:
            max_rev = by_author["paid_price"].max()
            popular_authors = by_author.loc[by_author["paid_price"] == max_rev, "_authors_list"].tolist()
        else:
            popular_authors = []
    else:
        popular_authors = []

    
    by_user = merged_df.groupby(user_key, as_index=False)["paid_price"].sum()
    if by_user.empty:
        best_buyer_aliases = []
        best_buyer_name = ""
        alias_names_list = []
    else:
        
        top_id = by_user.sort_values("paid_price", ascending=False).iloc[0][user_key]
        top_rows = merged_df[merged_df[user_key] == top_id]

        
        keys = ["email", "phone", "address", "name"]
        id_values: Dict[str, Set[str]] = {k: collect_values(top_rows, k) for k in keys}

        
        alias_ids: Set[str] = {str(top_id)}
        for k in keys:
            if not id_values[k]:
                continue
            match_cols = [c for c in merged_df.columns if k in c]
            for c in match_cols:
                values = merged_df[c].astype(str).str.strip()
                mask = values.isin(id_values[k])
                alias_ids.update(merged_df.loc[mask, user_key].astype(str).tolist())
        best_buyer_aliases = sorted(alias_ids)

        
        def name_for_id(uid: str) -> str:
            rows = merged_df[merged_df[user_key].astype(str) == str(uid)]
            for _, r in rows.iterrows():
                nm = best_display_name(r)
                if nm:
                    return nm
            return ""

        
        best_buyer_name = name_for_id(str(top_id))
        if not best_buyer_name:
            for aid in best_buyer_aliases:
                nm = name_for_id(aid)
                if nm:
                    best_buyer_name = nm
                    break

        
        alias_names: Set[str] = set()
        for aid in best_buyer_aliases:
            nm = name_for_id(aid)
            if nm:
                alias_names.add(nm)
        alias_names_list = sorted(alias_names)

    
    daily_chart = daily_revenue.sort_values("date").copy()
    daily_chart["date_dt"] = pd.to_datetime(daily_chart["date"].astype(str))
    fig = px.line(
        daily_chart, x="date_dt", y="total_revenue",
        title="Daily Revenue", markers=True
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=40, b=10),
        height=340,
        xaxis_title="Date",
        yaxis_title="Revenue (USD)"
    )
    chart_html = fig.to_html(full_html=False, include_plotlyjs="inline")

    
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Simple Static Dashboard</title>
  <style>
    :root {{ --bg:#0f172a; --panel:#111827; --text:#e5e7eb; --muted:#94a3b8; --accent:#22d3ee; }}
    body {{
      margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      background: radial-gradient(1200px 600px at 20% -50%, #0b1020 0, var(--bg) 60%), var(--bg);
      color: var(--text);
    }}
    .container {{ max-width: 1100px; margin: 24px auto; padding: 0 16px; }}
    header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom: 16px; }}
    .title {{ font-weight: 700; letter-spacing: 0.3px; }}
    .subtitle {{ color: var(--muted); font-size: 14px; }}
    .grid {{ display:grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }}
    .card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
      border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 14px;
    }}
    .card h3 {{ margin: 0 0 8px 0; }}
    .kpi {{ display:flex; flex-direction:column; gap: 6px; }}
    .kpi .label {{ color: var(--muted); font-size: 12px; }}
    .kpi .value {{ font-size: 20px; font-weight: 700; }}
    .list {{ margin: 0; padding-left: 18px; }}
    footer {{ margin-top: 20px; color: var(--muted); font-size: 12px; }}
    @media (max-width: 900px) {{ .grid {{ grid-template-columns: repeat(6, 1fr); }} }}
    @media (max-width: 600px) {{ .grid {{ grid-template-columns: repeat(1, 1fr); }} }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <div class="title">📊 Simple Static Dashboard</div>
      </div>
    </header>

    <!-- KPIs -->
    <section class="grid" style="margin-bottom: 8px;">
      <div class="card" style="grid-column: span 4;">
        <div class="kpi"><div class="label">Unique users</div><div class="value">{unique_users}</div></div>
      </div>
      <div class="card" style="grid-column: span 4;">
        <div class="kpi"><div class="label">Unique sets of authors</div><div class="value">{unique_author_sets}</div></div>
      </div>
      <div class="card" style="grid-column: span 4;">
        <div class="kpi">
          <div class="label">Best buyer (name)</div>
          <div class="value">{best_buyer_name if best_buyer_name else "—"}</div>
          <div class="label" style="margin-top:6px;">Alias IDs</div>
          <div class="value">[{", ".join(repr(x) for x in best_buyer_aliases)}]</div>
        </div>
      </div>
    </section>

    <!-- Top 5 & Popular authors -->
    <section class="grid">
      <div class="card" style="grid-column: span 6;">
        <h3>Top 5 days by revenue (YYYY-MM-dd)</h3>
        <ul class="list">
          {"".join(f"<li>{p['date']} — {p['revenue']:.2f}</li>" for p in top5_pairs)}
        </ul>
      </div>
      <div class="card" style="grid-column: span 6;">
        <h3>Most popular author(s)</h3>
        <div>{", ".join(popular_authors) if popular_authors else "—"}</div>
      </div>
    </section>

    <!-- Chart -->
    <section class="grid" style="margin-top: 16px;">
      <div class="card" style="grid-column: span 12;">
        {chart_html}
      </div>
    </section>


"""
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)
    return output_html


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a simple static dashboard from FP data")
    parser.add_argument("--folder", type=str, required=True,
                        help="Path to DATA1 folder (users.csv, books.yaml, orders.parquet)")
    parser.add_argument("--out", type=str, default="dashboard.html", help="Output HTML file path")
    args = parser.parse_args()

    out = build_dashboard(args.folder, args.out)
    print(f"Saved dashboard to: {out}")

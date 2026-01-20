
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from io import BytesIO

# -------------------------------
# Streamlit Setup
# -------------------------------
st.set_page_config(layout="wide")
st.title("⛏️ Mining Dashboard (Streamlit Cloud Safe)")

# -------------------------------
# Header auto-detection
# -------------------------------
def find_header(df):
    for i in range(min(50, len(df))):
        if "date" in df.iloc[i].astype(str).str.lower().values:
            return i
    return 0

# -------------------------------
# Excel Upload
# -------------------------------
file = st.file_uploader("Upload Excel", type=["xlsx"])
if not file:
    st.stop()

raw = pd.read_excel(file, header=None)
hdr = find_header(raw)

file.seek(0)
df = pd.read_excel(file, header=hdr)

df.columns = [str(c).strip() for c in df.columns]
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date").reset_index(drop=True)

mine_cols = [c for c in df.columns if c not in ("Date", "Day", "Total")]

for c in mine_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

if "Total" not in df:
    df["Total"] = df[mine_cols].sum(axis=1)

# -------------------------------
# Stats
# -------------------------------
def stats(s):
    x = s.dropna()
    q1, q3 = x.quantile([0.25, 0.75])
    return dict(
        mean=float(x.mean()),
        std=float(x.std()),
        median=float(x.median()),
        iqr=float(q3 - q1)
    )

# -------------------------------
# Outlier detection
# -------------------------------
def outlier_iqr(s, k):
    q1, q3 = s.quantile([0.25, 0.75])
    iqr = q3 - q1
    return (s < q1 - k * iqr) | (s > q3 + k * iqr)

def outlier_z(s, th):
    z = (s - s.mean()) / s.std()
    return z.abs() > th

def outlier_ma(s, w, pct):
    ma = s.rolling(w, min_periods=1).mean()
    pdiff = (s - ma).abs() / ma.abs() * 100
    return (pdiff > pct), pdiff, ma

# -------------------------------
# Sidebar
# -------------------------------
mine_sel = st.sidebar.multiselect("Mines", mine_cols, mine_cols)
show_total = st.sidebar.checkbox("Show Total", True)

k = st.sidebar.slider("IQR k", 0.5, 5.0, 1.5)
zth = st.sidebar.slider("Z threshold", 1.0, 5.0, 3.0)
w = st.sidebar.slider("MA window", 3, 20, 5)
pct = st.sidebar.slider("MA deviation (%)", 5, 60, 20)

ctype = st.sidebar.radio("Chart Type", ["Line", "Bar", "Stacked"])
deg = st.sidebar.select_slider("Trendline Degree", [0,1,2,3,4], 1)

# -------------------------------
# KPI Cards
# -------------------------------
st.subheader("📊 Summary Statistics")

targets = mine_sel + (["Total"] if show_total else [])

for m in targets:
    st.write(f"### {m}")
    stt = stats(df[m])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Mean", f"{stt['mean']:.2f}")
    c2.metric("Std", f"{stt['std']:.2f}")
    c3.metric("Median", f"{stt['median']:.2f}")
    c4.metric("IQR", f"{stt['iqr']:.2f}")

# -------------------------------
# Chart
# -------------------------------
st.subheader("📈 Charts")

fig, ax = plt.subplots(figsize=(11,4))

if ctype == "Stacked":
    bottom = np.zeros(len(df))
    for m in mine_sel:
        ax.bar(df["Date"], df[m], bottom=bottom, label=m)
        bottom += df[m].values
else:
    for m in mine_sel:
        if ctype == "Line":
            ax.plot(df["Date"], df[m], label=m)
        else:
            ax.bar(df["Date"], df[m], label=m, alpha=0.7)

if show_total and ctype != "Stacked":
    ax.plot(df["Date"], df["Total"], label="Total", linewidth=2)

# Trendline + outliers
target = "Total" if show_total else mine_sel[0]

if deg > 0:
    x = np.arange(len(df))
    coef = np.polyfit(x, df[target], deg)
    fit = np.poly1d(coef)(x)
    ax.plot(df["Date"], fit, label=f"Trend {deg}", linewidth=2)

iq = outlier_iqr(df[target], k)
zs = outlier_z(df[target], zth)
ma_f, pdiff, ma = outlier_ma(df[target], w, pct)

out = iq | zs | ma_f

ax.scatter(df["Date"][out], df[target][out], color="red", label="Outliers", zorder=5)

ax.legend()
ax.grid()
st.pyplot(fig)

# -------------------------------
# Anomaly Table
# -------------------------------
st.subheader("🚨 Anomaly Table")

direction = np.where(df[target] > ma, "spike",
             np.where(df[target] < ma, "drop", ""))

anom = pd.DataFrame({
    "Date": df["Date"],
    "Value": df[target],
    "IQR": iq,
    "Z-score": zs,
    "MA-outlier": ma_f,
    "PctDiff": pdiff,
    "Outlier": out,
    "Direction": direction
})

st.dataframe(anom)

# -------------------------------
# PNG Download (Cloud-safe)
# -------------------------------
st.subheader("⬇️ Download Chart as PNG")

def fig_to_png():
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    return buf

st.download_button(
    "Download PNG Chart",
    data=fig_to_png(),
    file_name="mining_chart.png",
    mime="image/png"
)

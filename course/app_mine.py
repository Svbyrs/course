import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for headless environments
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
import numpy as np
import math
from io import BytesIO

try:
    from scipy import stats as sps
except:
    sps = None

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet


st.set_page_config(layout="wide")
st.title("Mining Dashboard")

def find_header(df):
    for i in range(min(50, len(df))):
        if "date" in df.iloc[i].astype(str).str.lower().values:
            return i
    return 0

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

def stats(s):
    x = s.dropna()
    q1, q3 = x.quantile([0.25, 0.75])
    return dict(
        mean=x.mean(),
        std=x.std(),
        median=x.median(),
        iqr=q3 - q1,
        q1=q1,
        q3=q3,
    )

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
    return pdiff > pct, pdiff, ma

def grubbs(s, alpha):
    if sps is None:
        return pd.Series(False, index=s.index)

    x = s.copy().astype(float)
    mask = pd.Series(False, index=x.index)
    keep = x.dropna()

    while len(keep) >= 3:
        m, sd = keep.mean(), keep.std()
        if sd == 0:
            break
        d = (keep - m).abs()
        worst = d.idxmax()
        G = d[worst] / sd

        t = sps.t.ppf(1 - alpha / (2 * len(keep)), df=len(keep) - 2)
        Gc = (len(keep) - 1) / math.sqrt(len(keep)) * math.sqrt(t*t / (len(keep)-2 + t*t))

        if G > Gc:
            mask[worst] = True
            keep = keep.drop(worst)
        else:
            break

    return mask

mine_sel = st.sidebar.multiselect("Mines", mine_cols, mine_cols)
show_total = st.sidebar.checkbox("Show Total", True)

k = st.sidebar.slider("IQR k", 0.5, 5.0, 1.5)
zth = st.sidebar.slider("Z-score threshold", 1.0, 5.0, 3.0)
w = st.sidebar.slider("MA window", 3, 20, 5)
pct = st.sidebar.slider("MA deviation (%)", 5, 60, 20)
alpha = st.sidebar.select_slider("Grubbs alpha", [0.1, 0.05, 0.025, 0.01], 0.05)

chart_type = st.sidebar.radio("Chart Type", ["Line", "Bar", "Stacked"])
deg = st.sidebar.select_slider("Trendline Degree", [0, 1, 2, 3, 4], 1)

st.subheader("📊 Summary Statistics")

cols_all = mine_sel + (["Total"] if show_total else [])

for m in cols_all:
    st.write(f"### {m}")

    s = df[m]
    stt = stats(s)

    kpi = st.columns(4)
    kpi[0].metric("Mean", f"{stt['mean']:.2f}")
    kpi[1].metric("Std", f"{stt['std']:.2f}")
    kpi[2].metric("Median", f"{stt['median']:.2f}")
    kpi[3].metric("IQR", f"{stt['iqr']:.2f}")

st.subheader("📈 Charts")

fig, ax = plt.subplots(figsize=(11, 4))

if chart_type == "Stacked":
    bottom = np.zeros(len(df))
    for m in mine_sel:
        ax.bar(df["Date"], df[m], bottom=bottom, label=m)
        bottom += df[m].values
else:
    for m in mine_sel:
        if chart_type == "Line":
            ax.plot(df["Date"], df[m], label=m)
        else:
            ax.bar(df["Date"], df[m], label=m, alpha=0.7)

if show_total and chart_type != "Stacked":
    ax.plot(df["Date"], df["Total"], label="Total", linewidth=2)

target = "Total" if show_total else mine_sel[0]

if deg > 0:
    x = np.arange(len(df))
    c = np.polyfit(x, df[target], deg)
    fit = np.poly1d(c)(x)
    ax.plot(df["Date"], fit, label=f"Trend {deg}", linewidth=2)

iq = outlier_iqr(df[target], k)
zs = outlier_z(df[target], zth)
ma_f, pdiff, ma = outlier_ma(df[target], w, pct)
gr = grubbs(df[target], alpha)

out = iq | zs | ma_f | gr

ax.scatter(df["Date"][out], df[target][out], color="red", label="Outliers", zorder=3)

ax.legend()
ax.grid()
st.pyplot(fig)

st.subheader("📄 PDF Report")

def generate_pdf():
    buf = BytesIO()
    doc = SimpleDocTemplate(buf)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>Report – {target}</b>", styles["Title"]))
    story.append(Spacer(1, 10))

    # Add stats
    s = stats(df[target])
    for key, val in s.items():
        story.append(Paragraph(f"{key}: {val:.2f}", styles["Normal"]))
    story.append(Spacer(1, 10))

    # Add chart
    img = BytesIO()
    fig.savefig(img, format="png", dpi=120, bbox_inches="tight")
    img.seek(0)
    story.append(Image(img, width=420, height=160))
    story.append(Spacer(1, 10))

    # Anomalies section
    story.append(Paragraph("<b>Anomalies</b>", styles["Heading2"]))
    # Create anomaly dataframe
    anom_df = pd.DataFrame({
        'Date': df['Date'],
        'Value': df[target],
        'Outlier': out,
        'Direction': np.where(df[target].diff() > 0, 'Spike', 'Drop')
    })
    for idx in anom_df.index[anom_df["Outlier"]]:
        row = anom_df.loc[idx]
        story.append(Paragraph(
            f"{row['Date'].date()} – {row['Direction']} – {row['Value']:.2f}",
            styles["Normal"]
        ))

    doc.build(story)
    buf.seek(0)
    return buf

if st.button("Generate PDF"):
    pdf_buf = generate_pdf()
    st.download_button("Download PDF", pdf_buf, "report.pdf", mime="application/pdf")
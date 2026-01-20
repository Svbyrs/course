import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
from fpdf import FPDF
import io
import base64

st.set_page_config(layout="wide")
st.title("⛏️ Complete Mines Analysis")

uploaded = st.file_uploader("Upload Mining.xlsx", type="xlsx")
if uploaded:
    # Skip config rows, read data only
    df = pd.read_excel(uploaded, skiprows=10)
    
    # Fix column names for your file
    df.columns = ['Day', 'Date', 'LV426', 'Acheron', 'Thedus', 'LV223', 'Total']
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date']).reset_index(drop=True)
    
    mines = ['LV426', 'Acheron', 'Thedus', 'LV223', 'Total']
    st.success(f"✅ Loaded {len(df)} days")
    st.dataframe(df[mines].head())
    
    # CONTROLS
    col1, col2 = st.columns(2)
    with col1: chart_type = st.selectbox("Chart", ["line", "bar"])
    with col2: trend_deg = st.slider("Trend", 1, 4, 2)
    
    iqr_mult = st.slider("IQR", 1.5, 3.0, 1.5)
    z_thresh = st.slider("Z-Score", 2.0, 4.0, 3.0)
    
    if st.button("🚀 ANALYZE ALL", type="primary"):
        stats = {}
        anomaly_counts = {}
        
        for mine in mines:
            data = df[mine].dropna()
            
            # Stats
            stats[mine] = {
                'Mean': data.mean(),
                'Std': data.std(),
                'Median': data.median(),
                'IQR': data.quantile(0.75)-data.quantile(0.25)
            }
            
            # 4 anomaly tests - FIXED INDEXING
            Q1, Q3 = data.quantile([0.25, 0.75])
            
            # Create boolean masks aligned with df index
            iqr_mask = ((df[mine] < Q1-iqr_mult*(Q3-Q1)) | (df[mine] > Q3+iqr_mult*(Q3-Q1))).fillna(False)
            z_mask = np.abs((df[mine] - df[mine].mean()) / df[mine].std()) > z_thresh
            ma = df[mine].rolling(7, min_periods=1).mean()
            ma_mask = np.abs(df[mine] - ma) / ma > 0.2
            grubbs_mask = np.abs((df[mine] - df[mine].mean()) / df[mine].std()) > 3.5
            
            anomaly_counts[f'{mine}_IQR'] = iqr_mask.sum()
            anomaly_counts[f'{mine}_Z'] = z_mask.sum()
            anomaly_counts[f'{mine}_MA'] = ma_mask.sum()
            anomaly_counts[f'{mine}_Grubbs'] = grubbs_mask.sum()
        
        # DISPLAY
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 STATISTICS")
            st.dataframe(pd.DataFrame(stats).T.round(0))
        
        with col2:
            st.subheader("🔍 ANOMALIES")
            anomaly_df = pd.DataFrame([anomaly_counts]).T
            anomaly_df.columns = ['Count']
            st.dataframe(anomaly_df[anomaly_df['Count'] > 0].astype(int))
        
        # CHART - FIXED
        fig = px.line(df, x='Date', y=mines, title="Mine Output") if chart_type == "line" else px.bar(df, x='Date', y=mines)
        
        # Simple outlier dots (using IQR)
        for mine in mines[:4]:
            Q1, Q3 = df[mine].quantile([0.25, 0.75])
            outlier_mask = (df[mine] < Q1-1.5*(Q3-Q1)) | (df[mine] > Q3+1.5*(Q3-Q1))
            outliers = df[outlier_mask]
            if len(outliers) > 0:
                fig.add_scatter(x=outliers['Date'], y=outliers[mine], 
                              mode='markers', marker=dict(color='red', size=12),
                              name=f'{mine} outliers', showlegend=False)
        
        # Trendline
        x = np.arange(len(df))
        y = df['Total'].fillna(df['LV426'])
        coef = np.polyfit(x, y, trend_deg)
        trend = np.polyval(coef, x)
        fig.add_scatter(x=df['Date'], y=trend, name=f'Trend deg{trend_deg}',
                       line=dict(color='orange', width=3))
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Save for PDF
        st.session_state.update({'stats': stats, 'df': df, 'fig': fig, 'anomaly_counts': anomaly_counts})
        st.success("✅ Analysis complete!")
    
    # PDF REPORT
    if st.button("📄 PDF REPORT") and 'stats' in st.session_state:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, "Mines Analysis Report", ln=1, align="C")
        
        pdf.set_font("Arial", "", 12)
        pdf.cell(0, 10, "STATISTICS", ln=1)
        for mine, s in st.session_state.stats.items():
            pdf.cell(0, 8, f"{mine}: {s['Mean']:.0f} ± {s['Std']:.0f}", ln=1)
        
        pdf.cell(0, 10, "ANOMALIES", ln=1)
        for test, count in st.session_state.anomaly_counts.items():
            if count > 0:
                pdf.cell(0, 6, f"{test}: {int(count)}", ln=1)
        
        # Chart image
        img_bytes = st.session_state.fig.to_image(format='png', width=800, height=500)
        pdf.image(io.BytesIO(img_bytes), 10, pdf.get_y(), w=190)
        
        pdf_bytes = io.BytesIO(pdf.output(dest='S').encode('latin1'))
        st.download_button("📥 Download PDF", pdf_bytes.getvalue(), "mines_report.pdf")

st.caption("✅ Upload XLSX → ANALYZE ALL → PDF REPORT")

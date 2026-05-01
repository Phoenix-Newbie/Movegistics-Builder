import streamlit as st
import pandas as pd
import io
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

st.set_page_config(page_title="Movegistics Reports Builder", page_icon="📦", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }
.stApp { background-color: #0f1117; color: #e8eaf0; }
header[data-testid="stHeader"] { background: transparent; }
.block-container { padding-top: 2rem; padding-bottom: 2rem; }
.hero-title { font-size: 2rem; font-weight: 700; color: #fff; letter-spacing: -0.5px; margin-bottom: 0.2rem; }
.hero-sub { font-size: 0.85rem; color: #6b7280; font-family: 'JetBrains Mono', monospace; margin-bottom: 1.5rem; }
.version-badge { display:inline-block; background:#1e2330; border:1px solid #2d3448; color:#6b7280; font-size:0.7rem; font-family:'JetBrains Mono',monospace; padding:2px 10px; border-radius:20px; margin-left:10px; vertical-align:middle; }
.section-header { font-size:0.72rem; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#4b5563; margin-bottom:0.8rem; padding-bottom:0.4rem; border-bottom:1px solid #1e2330; }
.status-ok { color:#34d399; font-size:0.78rem; font-family:'JetBrains Mono',monospace; }
.status-wait { color:#f59e0b; font-size:0.78rem; font-family:'JetBrains Mono',monospace; }
[data-testid="stFileUploader"] { border:1px dashed #2d3448 !important; border-radius:8px !important; }
hr { border-color:#1e2330 !important; }
</style>
""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_ID = "1Ni9fhEN5F9nXAYDk5pXGIGAUMqbFeED1mxixMeBQNMQ"
SCOPES   = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Auth ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_sheets_service():
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=SCOPES
        )
        return build("sheets", "v4", credentials=creds)
    except Exception as e:
        st.error(f"Google Sheets connection failed: {e}")
        return None

# ── Write dataframe to a Google Sheet tab ─────────────────────────────────────
def write_sheet_tab(sheets_svc, tab_name, df):
    try:
        sheet_meta = sheets_svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        existing   = [s['properties']['title'] for s in sheet_meta['sheets']]

        if tab_name not in existing:
            sheets_svc.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
            ).execute()

        sheets_svc.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A1"
        ).execute()

        df_clean = df.fillna("").astype(str)
        values   = [df_clean.columns.tolist()] + df_clean.values.tolist()

        sheets_svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        return True
    except Exception as e:
        st.warning(f"Could not write tab '{tab_name}': {e}")
        return False

# ── Append a row to Merge Log ─────────────────────────────────────────────────
def log_merge(sheets_svc, run_id, ts, ai_rows, jo_rows, op_rows, merged_rows, merged_cols):
    try:
        tab_name   = "Merge Log"
        sheet_meta = sheets_svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        existing   = [s['properties']['title'] for s in sheet_meta['sheets']]

        if tab_name not in existing:
            sheets_svc.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
            ).execute()
            headers = [["Run ID", "Timestamp", "ActualIncome Rows",
                        "JobOverview Rows", "Opportunities Rows",
                        "Merged Rows", "Merged Columns"]]
            sheets_svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"'{tab_name}'!A1",
                valueInputOption="RAW",
                body={"values": headers}
            ).execute()

        new_row = [[run_id, ts, ai_rows, jo_rows, op_rows, merged_rows, merged_cols]]
        sheets_svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": new_row}
        ).execute()
        return True
    except Exception as e:
        st.warning(f"Could not write Merge Log: {e}")
        return False

# ── Merge helpers ─────────────────────────────────────────────────────────────
def load_clean(file_or_path):
    df = pd.read_excel(file_or_path, engine='openpyxl', header=None)
    for i in range(min(5, len(df))):
        non_null = df.iloc[i].dropna()
        if len(non_null) > 3 and any(isinstance(v, str) for v in non_null):
            if i + 1 < len(df):
                df.columns = df.iloc[i]
                df = df.iloc[i+1:].reset_index(drop=True)
                return df
    return df

def fix_duplicate_cols(df):
    seen = {}
    new_cols = []
    for c in df.columns:
        c = str(c) if not isinstance(c, str) else c
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols
    return df

def merge_files(f_ai, f_jo, f_op):
    ai = load_clean(f_ai)
    jo = load_clean(f_jo)
    op = load_clean(f_op)

    for df in [ai, jo, op]:
        fix_duplicate_cols(df)
        if '#' in df.columns:
            df.drop(columns=['#'], inplace=True)

    ai_raw = ai.copy()
    jo_raw = jo.copy()
    op_raw = op.copy()

    # Prepare ActualIncome
    ai.rename(columns={'Customer Id': 'Customer ID', 'Customer Name': 'Customer Name_ai'}, inplace=True)
    ai_bring = ['Work Order', 'Customer Name_ai', 'Move Coordinator', 'Move Type', 'Move Status',
                'Move Charges', 'Packing Charges', 'Crating Charges', 'Additional Charges',
                'Storage - One Time Charges', 'Storage Recurring - 1st Month Charges',
                'Valuation Charges', 'Discount', 'Service Tax', 'Tips', 'CC Fee', 'Grand Total']
    ai_slim = ai[[c for c in ai_bring if c in ai.columns]]

    # Prepare JobOverview
    jo.rename(columns={
        'Customer Id':  'Customer ID',
        'Account Name': 'Customer Name',
        'WO Date':      'Move Date',
        'Date Booked':  'Date Booked',
    }, inplace=True)
    jo.drop(columns=[c for c in ['Opportunity Name'] if c in jo.columns], inplace=True)

    # Merge 1: JO + AI
    m1 = pd.merge(jo, ai_slim, left_on='WO Id', right_on='Work Order', how='left')
    m1.drop(columns=['Work Order'], inplace=True)
    m1['Customer Name'] = m1['Customer Name'].fillna(m1.get('Customer Name_ai'))
    m1.drop(columns=[c for c in ['Customer Name_ai'] if c in m1.columns], inplace=True)

    # Prepare Opportunities
    op.rename(columns={
        'Cust. Id':    'Customer ID',
        'Opp. Amount': 'Estimated_op',
        'Move Date':   'Move Date_op',
        'Created Date':'Date Booked_op',
    }, inplace=True)
    op.drop(columns=[c for c in ['Opp. Name', 'Acct. Name', 'Expected Close Date',
                                  'Location Type_1', 'Move Status', 'Branch',
                                  'Lead Source', 'Owner'] if c in op.columns], inplace=True)
    op_bring = ['Customer ID', 'Opp. Ref', 'Estimated_op', 'Move Date_op', 'Date Booked_op',
                'Move Details', 'Phone Number', 'Email Address',
                'Origin Details', 'Location Type', 'Destination Details']
    op_slim = op[[c for c in op_bring if c in op.columns]]

    # Merge 2: + OP
    m2 = pd.merge(m1, op_slim, on='Customer ID', how='left')
    m2['Estimated'] = m2['Estimated'].fillna(m2.get('Estimated_op'))
    m2.drop(columns=[c for c in ['Estimated_op', 'Move Date_op', 'Date Booked_op'] if c in m2.columns], inplace=True)
    m2.drop_duplicates(inplace=True)
    m2.reset_index(drop=True, inplace=True)

    return m2, ai_raw, jo_raw, op_raw

def to_excel_bytes(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Merged Data')
    return buf.getvalue()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-title">📦 Movegistics Reports Builder<span class="version-badge">v1.5</span></div>
<div class="hero-sub">CRM Data Merger — JobOverview · ActualIncome · Opportunities</div>
""", unsafe_allow_html=True)
st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📂 Upload & Merge", "🔍 Explore & Filter", "💾 Export"])

# ══ TAB 1 ════════════════════════════════════════════════════════════════════
with tab1:
    left, right = st.columns([1.1, 1], gap="large")

    with left:
        st.markdown('<div class="section-header">📂 Upload CRM Files</div>', unsafe_allow_html=True)

        st.markdown("**① ActualIncome Work Order Report**")
        f1 = st.file_uploader("ActualIncome", type=["xlsx","xls"], key="f1", label_visibility="collapsed")
        if f1: st.markdown('<span class="status-ok">✓ Loaded</span>', unsafe_allow_html=True)

        st.markdown("<br>**② Job Overview Detail** *(base file)*", unsafe_allow_html=True)
        f2 = st.file_uploader("JobOverview", type=["xlsx","xls"], key="f2", label_visibility="collapsed")
        if f2: st.markdown('<span class="status-ok">✓ Loaded</span>', unsafe_allow_html=True)

        st.markdown("<br>**③ Opportunities By Stage**", unsafe_allow_html=True)
        f3 = st.file_uploader("Opportunities", type=["xlsx","xls"], key="f3", label_visibility="collapsed")
        if f3: st.markdown('<span class="status-ok">✓ Loaded</span>', unsafe_allow_html=True)

        if not (f1 and f2 and f3):
            st.markdown("<br><span class='status-wait'>⚠ Upload all 3 files to enable merge</span>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="section-header">⚙️ Merge & Save</div>', unsafe_allow_html=True)
        st.info(
            "**After merge, auto-saves to Google Sheets:**\n\n"
            "📊 `Merge Log` · `ActualIncome` · `JobOverview`\n\n"
            "📊 `Opportunities` · `Merged Data`\n\n"
            "*(No Excel file saved to Drive)*"
        )
        st.markdown("<br>", unsafe_allow_html=True)

        if st.button("🔗 Merge & Sync to Sheets", disabled=not (f1 and f2 and f3), use_container_width=True):
            try:
                # Step 1: Merge
                with st.spinner("Merging CRM files..."):
                    merged_df, ai_raw, jo_raw, op_raw = merge_files(f1, f2, f3)
                    st.session_state['df']          = merged_df
                    st.session_state['filtered_df'] = merged_df
                    st.success(f"✅ Merged! **{merged_df.shape[0]:,} rows** × **{merged_df.shape[1]} columns**")

                # Step 2: Write to Google Sheets
                sheets_svc = get_sheets_service()
                if sheets_svc:
                    with st.spinner("Syncing to Google Sheets..."):
                        ts     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                        tabs = {
                            "ActualIncome":  ai_raw,
                            "JobOverview":   jo_raw,
                            "Opportunities": op_raw,
                            "Merged Data":   merged_df,
                        }
                        for tab_name, df_tab in tabs.items():
                            write_sheet_tab(sheets_svc, tab_name, df_tab)

                        log_merge(
                            sheets_svc,
                            run_id      = run_id,
                            ts          = ts,
                            ai_rows     = len(ai_raw),
                            jo_rows     = len(jo_raw),
                            op_rows     = len(op_raw),
                            merged_rows = merged_df.shape[0],
                            merged_cols = merged_df.shape[1],
                        )

                        sheet_link = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
                        st.success("✅ All data synced to Google Sheets!")
                        st.markdown(f"📊 [Open Google Sheet]({sheet_link})")
                        st.session_state['sheet_link'] = sheet_link

            except Exception as e:
                st.error(f"Error: {e}")

        if 'sheet_link' in st.session_state:
            st.markdown(f"📊 **Last sync:** [Open Google Sheet]({st.session_state['sheet_link']})")

        if 'df' in st.session_state:
            df = st.session_state['df']
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Rows",   f"{df.shape[0]:,}")
            c2.metric("Columns",      f"{df.shape[1]}")
            c3.metric("Files Merged", "3")
            st.markdown('<div class="section-header" style="margin-top:1rem">Preview (first 10 rows)</div>', unsafe_allow_html=True)
            st.dataframe(df.head(10), use_container_width=True, height=280)

# ══ TAB 2 ════════════════════════════════════════════════════════════════════
with tab2:
    if 'df' not in st.session_state:
        st.info("Go to **Upload & Merge** tab first to load your data.")
    else:
        df = st.session_state['df']

        st.markdown('<div class="section-header">🔍 Filter Data</div>', unsafe_allow_html=True)
        fc1, fc2, fc3 = st.columns(3)

        with fc1:
            branches = ['All'] + sorted(df['Branch'].dropna().unique().tolist()) if 'Branch' in df.columns else ['All']
            sel_branch = st.selectbox("Branch", branches)
        with fc2:
            statuses = ['All'] + sorted(df['Job Status'].dropna().unique().tolist()) if 'Job Status' in df.columns else ['All']
            sel_status = st.selectbox("Job Status", statuses)
        with fc3:
            owners = ['All'] + sorted(df['Owner'].dropna().unique().tolist()) if 'Owner' in df.columns else ['All']
            sel_owner = st.selectbox("Owner", owners)

        fdf = df.copy()
        if sel_branch != 'All':  fdf = fdf[fdf['Branch'] == sel_branch]
        if sel_status != 'All':  fdf = fdf[fdf['Job Status'] == sel_status]
        if sel_owner != 'All':   fdf = fdf[fdf['Owner'] == sel_owner]

        st.session_state['filtered_df'] = fdf
        st.markdown(f"**{fdf.shape[0]:,} rows** match your filters")
        st.dataframe(fdf, use_container_width=True, height=420)

        st.markdown("---")
        st.markdown('<div class="section-header">📊 Summary</div>', unsafe_allow_html=True)
        s1, s2 = st.columns(2)

        with s1:
            if 'Grand Total' in fdf.columns:
                gt = pd.to_numeric(fdf['Grand Total'].astype(str).str.replace(r'[$,]','',regex=True), errors='coerce')
                st.metric("Total Grand Total", f"${gt.sum():,.2f}")
            if 'Job Status' in fdf.columns:
                st.write("**Jobs by Status**")
                jsc = fdf['Job Status'].value_counts().reset_index()
                jsc.columns = ['Status', 'Count']
                st.dataframe(jsc, use_container_width=True, hide_index=True)
        with s2:
            if 'Branch' in fdf.columns:
                st.write("**Jobs by Branch**")
                bc = fdf['Branch'].value_counts().reset_index()
                bc.columns = ['Branch', 'Count']
                st.dataframe(bc, use_container_width=True, hide_index=True)
            if 'Move Type' in fdf.columns:
                st.write("**Jobs by Move Type**")
                mc = fdf['Move Type'].value_counts().reset_index()
                mc.columns = ['Move Type', 'Count']
                st.dataframe(mc, use_container_width=True, hide_index=True)

# ══ TAB 3 ════════════════════════════════════════════════════════════════════
with tab3:
    if 'df' not in st.session_state:
        st.info("Go to **Upload & Merge** tab first to load your data.")
    else:
        df  = st.session_state['df']
        fdf = st.session_state.get('filtered_df', df)
        ts  = datetime.now().strftime("%Y%m%d_%H%M")

        st.markdown('<div class="section-header">💾 Download Data</div>', unsafe_allow_html=True)
        e1, e2 = st.columns(2)

        with e1:
            st.markdown("**Full Merged Dataset**")
            st.caption(f"{df.shape[0]:,} rows · {df.shape[1]} columns")
            st.download_button("⬇ Download Full Excel", to_excel_bytes(df),
                file_name=f"movegistics_full_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
            st.download_button("⬇ Download Full CSV", df.to_csv(index=False).encode(),
                file_name=f"movegistics_full_{ts}.csv", mime="text/csv", use_container_width=True)

        with e2:
            st.markdown("**Filtered Dataset** *(from Explore tab)*")
            st.caption(f"{fdf.shape[0]:,} rows · {fdf.shape[1]} columns")
            st.download_button("⬇ Download Filtered Excel", to_excel_bytes(fdf),
                file_name=f"movegistics_filtered_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
            st.download_button("⬇ Download Filtered CSV", fdf.to_csv(index=False).encode(),
                file_name=f"movegistics_filtered_{ts}.csv", mime="text/csv", use_container_width=True)

        if 'sheet_link' in st.session_state:
            st.markdown("---")
            st.markdown(f"📊 **Google Sheet:** [Open Merge Log & Data]({st.session_state['sheet_link']})")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div style="text-align:center;color:#2d3448;font-size:0.72rem;font-family:JetBrains Mono,monospace;">Movegistics Reports Builder • CRM Data Tool</div>', unsafe_allow_html=True)

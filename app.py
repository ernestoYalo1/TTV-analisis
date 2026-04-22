#!/usr/bin/env python3
"""
Time-to-Value (TTV) Analysis — Streamlit Web App
=================================================
Measures how long it takes from closing a Salesforce deal to customers
actively using the product (unique contacts in BigQuery conversations).

Tabs:
  1. General: All accounts — TTV table, metrics, chart
  2. Fast Track: Priority accounts only (see FASTTRACK_KEYWORDS in settings)
  3. Matching: Link Salesforce accounts to BigQuery bot_ids

Run:
    streamlit run app.py --server.port 8504
"""
import logging
import streamlit as st

from config.settings import FASTTRACK_KEYWORDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")

st.set_page_config(
    page_title="TTV Analysis — Yalo",
    page_icon="📊",
    layout="wide",
)

st.title("Time-to-Value Analysis")
st.caption("Salesforce close date → first 10 / 50 / 100 unique contacts in WhatsApp")

tab_general, tab_fasttrack, tab_matching = st.tabs(
    ["📊 General", "⚡ Fast Track", "🔗 Matching"]
)

with tab_general:
    from components.dashboard_tab import render as render_dashboard
    render_dashboard(title="Time-to-Value — General", table_key="general_table")

with tab_fasttrack:
    from components.dashboard_tab import render as render_dashboard
    render_dashboard(
        filter_keywords=FASTTRACK_KEYWORDS,
        title="Time-to-Value — Fast Track",
        table_key="fasttrack_table",
        show_chart=False,
    )

with tab_matching:
    from components.matching_tab import render as render_matching
    render_matching()

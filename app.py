#!/usr/bin/env python3
"""
Time-to-Value (TTV) Analysis — Streamlit Web App
=================================================
Measures how long it takes from closing a Salesforce deal to customers
actively using the product (unique contacts in BigQuery conversations).

Two tabs:
  1. Matching: Link Salesforce accounts to BigQuery bot_ids
  2. Dashboard: View TTV table, metrics, and charts

Run:
    streamlit run app.py --server.port 8504
"""
import logging
import streamlit as st

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")

st.set_page_config(
    page_title="TTV Analysis — Yalo",
    page_icon="📊",
    layout="wide",
)

st.title("Time-to-Value Analysis")
st.caption("Salesforce close date → first 10 / 50 / 100 unique contacts in WhatsApp")

tab_matching, tab_dashboard = st.tabs(["🔗 Matching", "📊 Dashboard"])

with tab_matching:
    from components.matching_tab import render as render_matching
    render_matching()

with tab_dashboard:
    from components.dashboard_tab import render as render_dashboard
    render_dashboard()

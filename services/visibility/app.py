import os
import requests
import streamlit as st

MCP_BASE_URL = os.getenv("MCP_BASE_URL", "http://mcp-server:8080")

st.title("CogniPulse – Live Dashboard (Postgres-only)")
st.caption(f"MCP Server: {MCP_BASE_URL}")

if st.button("Ping MCP /health"):
    try:
        r = requests.get(f"{MCP_BASE_URL}/health", timeout=3)
        st.write(r.json())
    except Exception as e:
        st.error(str(e))

col1, col2 = st.columns(2)

with col1:
    st.subheader("Recent Alerts")
    limit = st.slider("Alert limit", 5, 50, 20)
    try:
        alerts = requests.get(f"{MCP_BASE_URL}/alerts/recent", params={"limit": limit}, timeout=3).json()
        st.json(alerts)
    except Exception as e:
        st.error(str(e))

with col2:
    st.subheader("Recent Actions")
    limit2 = st.slider("Action limit", 5, 50, 20)
    try:
        actions = requests.get(f"{MCP_BASE_URL}/actions/recent", params={"limit": limit2}, timeout=3).json()
        st.json(actions)
    except Exception as e:
        st.error(str(e))

st.subheader("Device State")
device_id = st.text_input("Device ID", "dev-1")

try:
    state = requests.get(f"{MCP_BASE_URL}/state/{device_id}", timeout=3).json()
    st.json(state)
except Exception as e:
    st.error(str(e))

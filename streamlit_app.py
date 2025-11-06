"""
Streamlit Dashboard Viewer
Interactive UI for viewing PE dashboards
"""

import streamlit as st
import requests
from pathlib import Path
import json
import os

# API Configuration - use environment variable for Docker, fallback to localhost
API_BASE = os.getenv("API_BASE", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="PE Dashboard - Forbes AI 50",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .pipeline-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.85rem;
        font-weight: bold;
        margin-left: 0.5rem;
    }
    .badge-rag {
        background-color: #ff7f0e;
        color: white;
    }
    .badge-structured {
        background-color: #2ca02c;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


def fetch_companies():
    """Fetch list of available companies from API"""
    try:
        response = requests.get(f"{API_BASE}/companies", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch companies: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Cannot connect to API. Make sure FastAPI is running on {API_BASE}")
        st.error(f"Error: {str(e)}")
        return None


def generate_dashboard(company_id: str, pipeline: str, model: str = "gpt-4o-mini"):
    """Generate dashboard via API"""
    endpoint = f"{API_BASE}/dashboard/{pipeline}"
    
    with st.spinner(f"Generating {pipeline.upper()} dashboard for {company_id}..."):
        try:
            response = requests.post(
                endpoint,
                json={"company_id": company_id, "model": model},
                timeout=60
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"Error: {response.status_code}")
                st.error(response.json().get('detail', 'Unknown error'))
                return None
                
        except Exception as e:
            st.error(f"Error generating dashboard: {str(e)}")
            return None


def main():
    """Main Streamlit app"""
    
    # Header
    st.markdown('<p class="main-header">📊 PE Dashboard Factory</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Forbes AI 50 Intelligence Platform • Quanta Capital Partners</p>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("⚙️ Dashboard Settings")
    
    # Fetch companies
    companies_data = fetch_companies()
    
    if not companies_data:
        st.warning("⚠️ Cannot connect to API. Please start the FastAPI server:")
        st.code("python app.py", language="bash")
        st.stop()
    
    companies = companies_data.get('companies', [])
    
    if not companies:
        st.error("No companies found in the system")
        st.stop()
    
    # Company selector
    st.sidebar.subheader("Select Company")
    
    # Create dropdown options
    company_options = {
        f"{c['company_name']} ({c['category']})": c['company_id'] 
        for c in companies
    }
    
    selected_display = st.sidebar.selectbox(
        "Choose a company:",
        options=list(company_options.keys()),
        index=0
    )
    
    selected_company_id = company_options[selected_display]
    selected_company = next(c for c in companies if c['company_id'] == selected_company_id)
    
    # Pipeline selector
    st.sidebar.subheader("Select Pipeline")
    view_mode = st.sidebar.radio(
        "Dashboard Type:",
        ["RAG Pipeline", "Structured Pipeline", "Compare Both"],
        index=0
    )
    
    # Model selector
    st.sidebar.subheader("Model Settings")
    model = st.sidebar.selectbox(
        "LLM Model:",
        ["gpt-4o-mini", "gpt-4o"],
        index=0,
        help="gpt-4o is more powerful but more expensive"
    )
    
    # Display company info
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Company", selected_company['company_name'])
    with col2:
        st.metric("Category", selected_company['category'])
    with col3:
        st.metric("Website", "🔗")
        st.markdown(f"[Visit]({selected_company['website']})", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Generate button
    if st.button("🚀 Generate Dashboard", type="primary", use_container_width=True):
        
        if view_mode == "Compare Both":
            # Generate both pipelines
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("RAG Pipeline")
                st.markdown('<span class="pipeline-badge badge-rag">RAG</span>', unsafe_allow_html=True)
                rag_result = generate_dashboard(selected_company_id, "rag", model)
                if rag_result:
                    st.markdown(rag_result['dashboard'])
            
            with col2:
                st.subheader("Structured Pipeline")
                st.markdown('<span class="pipeline-badge badge-structured">Structured</span>', unsafe_allow_html=True)
                structured_result = generate_dashboard(selected_company_id, "structured", model)
                if structured_result:
                    st.markdown(structured_result['dashboard'])
        
        else:
            # Single pipeline
            pipeline = "rag" if "RAG" in view_mode else "structured"
            pipeline_name = "RAG" if pipeline == "rag" else "Structured"
            badge_class = "badge-rag" if pipeline == "rag" else "badge-structured"
            
            st.subheader(f"{pipeline_name} Dashboard")
            st.markdown(f'<span class="pipeline-badge {badge_class}">{pipeline_name}</span>', unsafe_allow_html=True)
            
            result = generate_dashboard(selected_company_id, pipeline, model)
            
            if result:
                st.markdown(result['dashboard'])
                
                # Download button
                st.download_button(
                    label="📥 Download Dashboard",
                    data=result['dashboard'],
                    file_name=f"{selected_company_id}_{pipeline}_dashboard.md",
                    mime="text/markdown"
                )
    
    # Sidebar info
    st.sidebar.markdown("---")
    st.sidebar.subheader("📈 System Stats")
    st.sidebar.metric("Total Companies", len(companies))
    st.sidebar.metric("API Status", "✅ Connected" if companies_data else "❌ Disconnected")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.caption("Project ORBIT • Quanta Capital Partners")
    st.sidebar.caption("Automated PE Intelligence System")


if __name__ == "__main__":
    main()
# Project ORBIT — PE Dashboard for Forbes AI 50

This is the starter package for **Assignment 2 — DAMG7245**.

## Run locally (dev)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn src.api:app --reload
# in another terminal
streamlit run src/streamlit_app.py
```

## Docker (app layer only)

```bash
cd docker
docker compose up --build
```

This starts:
- FastAPI: http://localhost:8000
- Streamlit: http://localhost:8501

# Add instructions on running on the cloud based on your setup and links to Codelabs, architecture diagrams etc.

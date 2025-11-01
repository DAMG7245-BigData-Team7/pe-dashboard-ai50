# Assignment 2 — DAMG7245
## Case Study 2 — Project ORBIT (Part 1)
**Automating Private-Equity (PE) Intelligence for the Forbes AI 50**

### Setting
Quanta Capital Partners, a growth-stage investment firm, tracks the **Forbes AI 50** startups. Analysts currently open each company website, LinkedIn page, and press/blog page to collect basic investment signals: HQ, founding year, funding rounds, hiring momentum, leadership changes, and product focus. They then hand-write an investor-style diligence note.

This process:
- does not scale to all 50 companies,
- is hard to refresh daily, and
- is inconsistent across analysts.

To fix this, **Priya Rao, VP of Data Engineering**, launches **Project ORBIT**. Her goal: **build an automated, reproducible, cloud-hosted system** that can:

1. Ingest public data for **all Forbes AI 50** companies,
2. Build **two parallel generation pipelines**:
   - Unstructured → RAG → LLM → PE Dashboard
   - Structured (Pydantic + Instructor) → LLM → PE Dashboard
3. Compare the two dashboards for quality,
4. Run an **initial/full** pipeline and a **daily/update** pipeline in **Airflow**, and
5. Serve dashboards through **FastAPI + Streamlit** (dockerized) on **GCP or AWS**.

By the end of this assignment, the “Quanta team” (you) must deliver a **PE Dashboard Factory**.

---

## Learning Outcomes
- Build ingestion/orchestration with **Apache Airflow** (initial + daily)
- Build **RAG** with a vector database
- Build **structured-output** extraction with **Pydantic + instructor-style prompting**
- Design **LLM prompts** that emit investor dashboards in a fixed schema
- **Compare** 2 LLM pipelines with a rubric
- **Dockerize** FastAPI + Streamlit

---

## Phase 1 – Data Ingestion & Pipeline Bootstrap (Labs 0–3)

### Lab 0 — Project bootstrap & AI 50 seed
**Goal:** create a reproducible repo and seed list.

**Tasks**
1. Create a Git repo with folders:
   - `src/`, `dags/`, `data/`, `app/` (optional), `docker/`
2. Add `data/forbes_ai50_seed.json` (provided) — this has the **schema only**.  
   You must *populate it yourself* with the current Forbes AI 50 from https://www.forbes.com/lists/ai50/ .
3. Add a `README.md` with run instructions.

**Checkpoint**
- `git status` is clean
- `data/forbes_ai50_seed.json` exists

---

### Lab 1 — Scrape & store
**Goal:** pull source pages and store them to cloud (GCS or S3) or locally for dev.I recommend you create a folder for each company with subfolders for the initial pull and subsequent daily runs

**Tasks**
1. Write a Python scraper to fetch:
   - homepage
   - /about
   - /product or /platform
   - /careers
   - /blog or /news
2. Save **raw HTML** and a **clean-text** version
3. Emit metadata:
   ```json
   {
     "company_name": "...",
     "source_url": "...",
     "crawled_at": "2025-10-31T10:00:00Z"
   }
   ```

**Checkpoint**
- Companies scraped into `data/raw/<company_id>/...`

---

### Lab 2 — Full-load Airflow DAG
**Goal:** run ingestion for **all** AI 50.

**Tasks**
1. Create `dags/ai50_full_ingest_dag.py` with tasks:
   - load_company_list
   - scrape_company_pages (mapped / TaskGroup)
   - store_raw_to_cloud
2. Schedule: `@once`
3. Output: `raw/<company_id>/...` + metadata

**Checkpoint**
- DAG runs and completes for the AI50 companies
---

### Lab 3 — Daily/update Airflow DAG
**Goal:** refresh deltas daily.

**Tasks**
1. Create `dags/ai50_daily_refresh_dag.py`
2. Schedule: `0 3 * * *`
3. Re-scrape only changed or key pages (About, Careers, Blog)
4. Create subfolders for each run in the company folder
5. Log success/failure per company

**Checkpoint**
- DAG runs without breaking the full-load run

---

## Phase 2 – Knowledge Representation (Labs 4–6)

### Lab 4 — Vector DB & RAG index
**Goal:** support unstructured, retrieval-augmented dashboards.

**Tasks**
1. Chunk raw text (500–1,000 tokens)
2. Embed chunks and store in a local vector DB (FAISS, Chroma, Qdrant)
3. Add a FastAPI endpoint `/rag/search` to test retrieval

**Checkpoint**
- Querying “funding” or “leadership” for a company returns the right chunk

---

### Lab 5 — Structured extraction with Pydantic
**Goal:** normalize messy web text into clean objects to feed the LLM.

**Tasks**
1. Use `src/models.py` (provided) — Company, Event, Snapshot, Product, Leadership, Visibility
2. For each scraped source, call the LLM with an **instructor-style** prompt to fill the Pydantic model
3. Save results as `data/structured/<company_id>.json`

**Checkpoint**
- At least 5 companies with full payloads (company + events + snapshots ...)
- We should be able to try it for any of the 50 companies
---

### Lab 6 — Payload assembly
**Goal:** build the exact payload that the dashboard prompt expects.

**Tasks**
1. Combine:
   - `company_record`
   - `events`
   - `snapshots`
   - `products`
   - `leadership`
   - `visibility`
   - `notes`
   - `provenance_policy`
2. Save to `data/payloads/<company_id>.json`

**Checkpoint**
- Payload validates and can be loaded by `src/structured_pipeline.py`

---

## Phase 3 – Dashboard Generation (Labs 7–9)

### Lab 7 — RAG Pipeline Dashboard
**Goal:** raw pages → vector DB → LLM → Markdown dashboard

**Tasks**
1. Implement `POST /dashboard/rag` in FastAPI
2. Retrieve top-k context for the company
3. Call LLM with the dashboard prompt (`src/prompts/dashboard_system.md`)
4. Enforce the 8-section output:

   1. ## Company Overview
   2. ## Business Model and GTM
   3. ## Funding & Investor Profile
   4. ## Growth Momentum
   5. ## Visibility & Market Sentiment
   6. ## Risks and Challenges
   7. ## Outlook
   8. ## Disclosure Gaps

**Checkpoint**
- “Not disclosed.” is used when data is missing

---

### Lab 8 — Structured Pipeline Dashboard
**Goal:** structured payload → LLM → Markdown dashboard

**Tasks**
1. Implement `POST /dashboard/structured`
2. Load `data/payloads/<company_id>.json`
3. Pass as context to LLM with the *same* prompt
4. Return Markdown

**Checkpoint**
- Output is more precise and less hallucinatory than RAG

---

### Lab 9 — Evaluation & Comparison
**Goal:** compare RAG vs Structured for at least **5 companies**.

**Tasks**
1. Use the rubric below
2. Fill out `EVAL.md`
3. Write 1-page reflection in the repo

**Rubric (10 points):**
- Factual correctness (0–3)
- Schema adherence (0–2)
- Provenance use (0–2)
- Hallucination control (0–2)
- Readability / investor usefulness (0–1)

---

## Phase 4 – Deployment & Automation (Labs 10–11)

### Lab 10 — Dockerize FastAPI + Streamlit
**Goal:** run app in container on **GCP or AWS**.

**Tasks**
1. Use provided Dockerfile (FastAPI + Streamlit only)
2. `docker build -t pe-dashboard .`
3. `docker-compose up` should:
   - start FastAPI at `http://localhost:8000`
   - start Streamlit at `http://localhost:8501`

**Checkpoint**
- Both apps run locally in Docker

---

### Lab 11 — DAG ↔ App integration
**Goal:** make data refresh visible in the app.

**Tasks**
1. At the end of the daily DAG, write latest payload to `data/payloads/`
2. App should read from that folder (or cloud bucket)
3. (Optional) Add notification if dashboard generation fails

**Checkpoint**
- After daily run, Streamlit shows updated company list

---

## Deliverables

1. **GitHub repo** named `pe-dashboard-ai50`
2. **Working FastAPI** (`/companies`, `/dashboard/rag`, `/dashboard/structured`)
3. **Working Streamlit** (dropdown → dashboard)
4. **Two Airflow DAGs** (`ai50_full_ingest_dag.py`, `ai50_daily_refresh_dag.py`)
5. **Docker** for FastAPI + Streamlit
6. **EVAL.md** with at least 5 companies
7. **Demo video ≤10 mins** (hosted, link in README)
8. **Contribution attestation** (provided template)

---

## Important Notes
- You must use **all 50** from Forbes AI 50
- If a field cannot be found → **“Not disclosed.”**
- **Never invent** ARR, MRR, valuation, customer logos, or pipeline.
- Always include **“## Disclosure Gaps”**.

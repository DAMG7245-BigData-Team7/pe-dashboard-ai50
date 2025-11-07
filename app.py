"""
FastAPI Application for PE Dashboard Generation
Labs 7 & 8 - RAG and Structured Pipeline Endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
from typing import Optional, List
import os
import json
from openai import OpenAI
from dotenv import load_dotenv

from src.vector_db_pinecone import PineconeRetriever
from src.utils import logger, load_json, ScraperConfig

from google.cloud import storage

# Load environment
load_dotenv()

# Environment configuration
USE_GCS = os.getenv("USE_GCS", "false").lower() == "true"
GCS_BUCKET = os.getenv("DATA_BUCKET", "pe-dashboard-data")

logger.info(f"🚀 Starting PE Dashboard API")
logger.info(f"📦 Storage mode: {'GCS' if USE_GCS else 'Local filesystem'}")
if USE_GCS:
    logger.info(f"☁️  GCS Bucket: {GCS_BUCKET}")

app = FastAPI(
    title="PE Dashboard API",
    description="Generate investor dashboards for Forbes AI 50 companies",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Load dashboard prompt template
PROMPT_FILE = Path("dashboard_prompt.txt")
with open(PROMPT_FILE, 'r') as f:
    DASHBOARD_PROMPT_TEMPLATE = f.read()


class DashboardRequest(BaseModel):
    """Request model for dashboard generation"""
    company_id: str
    model: Optional[str] = "gpt-4o-mini"


class DashboardResponse(BaseModel):
    """Response model for dashboard"""
    company_id: str
    company_name: str
    dashboard: str
    pipeline: str
    model_used: str


# ============================================================================
# GCS HELPER FUNCTIONS
# ============================================================================

def get_gcs_client():
    """Get GCS client with error handling"""
    try:
        return storage.Client()
    except Exception as e:
        logger.error(f"❌ Failed to initialize GCS client: {e}")
        return None


def list_company_files_from_gcs(bucket_name: str) -> list:
    """List all company JSON files in GCS bucket"""
    try:
        client = get_gcs_client()
        if not client:
            return []
        
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix='structured/')
        
        files = []
        for blob in blobs:
            if blob.name.endswith('.json') and blob.name != 'structured/':
                files.append(blob.name)
        
        logger.info(f"✅ Found {len(files)} files in GCS bucket {bucket_name}")
        return files
    
    except Exception as e:
        logger.error(f"❌ Error listing GCS files: {e}")
        return []


def load_company_from_gcs(bucket_name: str, company_id: str) -> dict:
    """Load company JSON from GCS"""
    try:
        client = get_gcs_client()
        if not client:
            return None
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f'structured/{company_id}.json')
        
        if not blob.exists():
            logger.warning(f"⚠️  Company {company_id} not found in GCS")
            return None
        
        content = blob.download_as_text()
        data = json.loads(content)
        logger.info(f"✅ Loaded {company_id} from GCS")
        return data
    
    except Exception as e:
        logger.error(f"❌ Error loading {company_id} from GCS: {e}")
        return None


def load_company_from_local(company_id: str) -> dict:
    """Load company JSON from local filesystem"""
    try:
        file_path = ScraperConfig.DATA_DIR / "structured" / f"{company_id}.json"
        if not file_path.exists():
            logger.warning(f"⚠️  Company {company_id} not found locally")
            return None
        
        data = load_json(file_path)
        logger.info(f"✅ Loaded {company_id} from local filesystem")
        return data
    
    except Exception as e:
        logger.error(f"❌ Error loading {company_id} locally: {e}")
        return None


def load_company_data(company_id: str) -> dict:
    """Load company data from GCS or local (based on USE_GCS flag)"""
    if USE_GCS:
        return load_company_from_gcs(GCS_BUCKET, company_id)
    else:
        return load_company_from_local(company_id)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/")
def read_root():
    """Root endpoint"""
    return {
        "message": "PE Dashboard API",
        "version": "1.0.0",
        "storage_mode": "GCS" if USE_GCS else "Local",
        "endpoints": {
            "companies": "/companies",
            "rag_dashboard": "/dashboard/rag",
            "structured_dashboard": "/dashboard/structured",
            "health": "/health"
        }
    }


@app.get("/companies")
def list_companies():
    """List all available companies from GCS or local storage"""
    
    companies = []
    
    if USE_GCS:
        # Production: Read from GCS
        logger.info(f"📋 Listing companies from GCS bucket: {GCS_BUCKET}")
        
        try:
            client = get_gcs_client()
            if not client:
                return {
                    "companies": [],
                    "count": 0,
                    "storage_mode": "GCS",
                    "error": "GCS client initialization failed"
                }
            
            bucket = client.bucket(GCS_BUCKET)
            blobs = bucket.list_blobs(prefix='structured/')
            
            for blob in blobs:
                if blob.name.endswith('.json') and blob.name != 'structured/':
                    try:
                        content = blob.download_as_text()
                        data = json.loads(content)
                        companies.append({
                            "company_id": data['company']['company_id'],
                            "company_name": data['company']['company_name'],
                            "website": data['company']['website'],
                            "category": data['company'].get('category', 'Not disclosed')
                        })
                    except Exception as e:
                        logger.error(f"❌ Error loading {blob.name} from GCS: {e}")
            
            logger.info(f"✅ Successfully loaded {len(companies)} companies from GCS")
        
        except Exception as e:
            logger.error(f"❌ Error listing companies from GCS: {e}")
            return {
                "companies": [],
                "count": 0,
                "storage_mode": "GCS",
                "error": str(e)
            }
    
    else:
        # Development: Read from local filesystem
        logger.info("📋 Listing companies from local filesystem")
        
        structured_dir = ScraperConfig.DATA_DIR / "structured"
        
        if not structured_dir.exists():
            logger.warning(f"⚠️  Local structured directory not found: {structured_dir}")
            return {
                "companies": [],
                "count": 0,
                "storage_mode": "Local"
            }
        
        for json_file in sorted(structured_dir.glob("*.json")):
            try:
                data = load_json(json_file)
                companies.append({
                    "company_id": data['company']['company_id'],
                    "company_name": data['company']['company_name'],
                    "website": data['company']['website'],
                    "category": data['company'].get('category', 'Not disclosed')
                })
            except Exception as e:
                logger.error(f"❌ Error loading {json_file}: {e}")
        
        logger.info(f"✅ Successfully loaded {len(companies)} companies from local filesystem")
    
    return {
        "companies": companies,
        "count": len(companies),
        "storage_mode": "GCS" if USE_GCS else "Local"
    }


@app.post("/dashboard/rag", response_model=DashboardResponse)
def generate_rag_dashboard(request: DashboardRequest):
    """
    Lab 7 - Generate dashboard using RAG pipeline
    Retrieves chunks from vector DB and generates dashboard
    """
    
    try:
        logger.info(f"🔍 Generating RAG dashboard for: {request.company_id}")
        
        # Initialize retriever
        retriever = PineconeRetriever()
        
        # Retrieve relevant chunks for the company
        queries = [
            "company overview and description",
            "funding rounds and investors",
            "products and business model",
            "growth metrics and hiring",
            "partnerships and news"
        ]
        
        all_chunks = []
        for query in queries:
            chunks = retriever.search(
                query=query,
                company_id=request.company_id,
                k=3
            )
            all_chunks.extend(chunks)
        
        # Remove duplicates
        seen_texts = set()
        unique_chunks = []
        for chunk in all_chunks:
            if chunk['text'] not in seen_texts:
                seen_texts.add(chunk['text'])
                unique_chunks.append(chunk)
        
        if not unique_chunks:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for company: {request.company_id}"
            )
        
        logger.info(f"📚 Retrieved {len(unique_chunks)} unique chunks from Pinecone")
        
        # Combine chunks into context
        context = "\n\n---\n\n".join([
            f"SOURCE: {chunk['metadata']['page_type']}\n{chunk['text']}"
            for chunk in unique_chunks[:15]
        ])
        
        # Generate dashboard
        prompt = DASHBOARD_PROMPT_TEMPLATE.format(company_name=request.company_id)
        
        full_prompt = f"""{prompt}

RETRIEVED CONTEXT:
{context}

Generate the complete 8-section investor dashboard now."""
        
        logger.info(f"🤖 Calling OpenAI API with model: {request.model}")
        
        response = openai_client.chat.completions.create(
            model=request.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert private equity analyst. Generate accurate, well-structured investor dashboards."
                },
                {
                    "role": "user",
                    "content": full_prompt
                }
            ],
            temperature=0.3,
            max_tokens=2500
        )
        
        dashboard = response.choices[0].message.content
        
        logger.info(f"✅ Successfully generated RAG dashboard for {request.company_id}")
        
        return DashboardResponse(
            company_id=request.company_id,
            company_name=request.company_id,
            dashboard=dashboard,
            pipeline="RAG",
            model_used=request.model
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating RAG dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dashboard/structured", response_model=DashboardResponse)
def generate_structured_dashboard(request: DashboardRequest):
    """
    Lab 8 - Generate dashboard using Structured pipeline
    Uses pre-extracted Pydantic payload from GCS or local storage
    """
    
    try:
        logger.info(f"📊 Generating Structured dashboard for: {request.company_id}")
        
        # Load from GCS or local based on USE_GCS flag
        payload = load_company_data(request.company_id)
        
        if not payload:
            raise HTTPException(
                status_code=404,
                detail=f"No structured data found for: {request.company_id}"
            )
        
        logger.info(f"✅ Loaded structured data for {request.company_id} from {'GCS' if USE_GCS else 'local'}")
        
        # Helper function to format funding rounds
        def format_rounds(rounds):
            if not rounds:
                return "Not disclosed"
            formatted = []
            for r in rounds[:5]:
                date = r.get('date', 'Date unknown')
                stage = r.get('stage', 'Unknown stage')
                amount = r.get('amount', 'Not disclosed')
                lead = r.get('lead_investor', 'Not disclosed')
                formatted.append(f"- {date}: {stage} - {amount} (Lead: {lead})")
            return '\n'.join(formatted)
        
        # Helper function to format events
        def format_evts(events):
            if not events:
                return "No recent events found"
            formatted = []
            for e in events[:10]:
                date = e.get('date', 'Date unknown')
                event_type = e.get('event_type', 'Other')
                title = e.get('title', 'Untitled event')
                formatted.append(f"- {date} ({event_type}): {title}")
            return '\n'.join(formatted)
        
        # Format payload as context
        context = f"""
COMPANY INFORMATION:
Name: {payload['company']['company_name']}
Website: {payload['company']['website']}
Founded: {payload['company'].get('founded_year', 'Not disclosed')}
HQ: {payload['company'].get('hq_city', 'Not disclosed')}, {payload['company'].get('hq_country', 'Not disclosed')}
Category: {payload['company'].get('category', 'Not disclosed')}
Description: {payload['company']['description']}
Business Model: {payload['company'].get('business_model', 'Not disclosed')}
Target Customers: {payload['company'].get('target_customers', 'Not disclosed')}
Pricing: {payload['company'].get('pricing_model', 'Not disclosed')}
Competitors: {', '.join(payload['company'].get('competitors', []))}

SNAPSHOT (as of {payload['snapshot']['snapshot_date']}):
Total Funding: {payload['snapshot'].get('total_funding', 'Not disclosed')}
Last Round: {payload['snapshot'].get('last_funding_date', 'Not disclosed')} ({payload['snapshot'].get('last_funding_stage', 'Not disclosed')})
Valuation: {payload['snapshot'].get('valuation', 'Not disclosed')}
Headcount: {payload['snapshot'].get('headcount', 'Not disclosed')}
Customer Count: {payload['snapshot'].get('customer_count', 'Not disclosed')}
Revenue: {payload['snapshot'].get('revenue_range', 'Not disclosed')}

FUNDING ROUNDS:
{format_rounds(payload.get('funding_rounds', []))}

GROWTH METRICS:
Headcount: {payload['growth_metrics'].get('headcount', 'Not disclosed')}
Headcount Growth YoY: {payload['growth_metrics'].get('headcount_growth_yoy', 'Not disclosed')}
Open Roles: {payload['growth_metrics'].get('open_roles', 'Not disclosed')}
Office Locations: {', '.join(payload['growth_metrics'].get('office_locations', []))}
Geographic Expansion: {payload['growth_metrics'].get('geographic_expansion', 'Not disclosed')}
Partnerships: {', '.join(payload['growth_metrics'].get('recent_partnerships', []))}
Recent Products: {', '.join(payload['growth_metrics'].get('recent_products', []))}

RECENT EVENTS:
{format_evts(payload.get('events', []))}

INVESTOR PROFILE:
Total Raised: {payload['investor_profile'].get('total_raised', 'Not disclosed')}
Lead Investors: {', '.join(payload['investor_profile'].get('lead_investors', []))}
All Investors: {', '.join(payload['investor_profile'].get('all_investors', []))}

DISCLOSURE GAPS:
{', '.join(payload['disclosure_gaps'].get('missing_fields', []))}
"""
        
        # Generate dashboard
        prompt = DASHBOARD_PROMPT_TEMPLATE.format(company_name=payload['company']['company_name'])
        
        full_prompt = f"""{prompt}

STRUCTURED DATA:
{context}

Generate the complete 8-section investor dashboard now."""
        
        logger.info(f"🤖 Calling OpenAI API with model: {request.model}")
        
        response = openai_client.chat.completions.create(
            model=request.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert private equity analyst. Generate accurate, well-structured investor dashboards based on structured data."
                },
                {
                    "role": "user",
                    "content": full_prompt
                }
            ],
            temperature=0.3,
            max_tokens=2500
        )
        
        dashboard = response.choices[0].message.content
        
        logger.info(f"✅ Successfully generated Structured dashboard for {request.company_id}")
        
        return DashboardResponse(
            company_id=request.company_id,
            company_name=payload['company']['company_name'],
            dashboard=dashboard,
            pipeline="Structured",
            model_used=request.model
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating structured dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "storage_mode": "GCS" if USE_GCS else "Local",
        "gcs_bucket": GCS_BUCKET if USE_GCS else None
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
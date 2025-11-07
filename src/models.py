import re  # ← ADDED: Required for parse_funding_amount
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class FundingStage(str, Enum):
    """Funding stage categories"""
    SEED = "seed"
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    SERIES_D_PLUS = "series_d_plus"
    GROWTH = "growth"
    LATE_STAGE = "late_stage"
    UNKNOWN = "unknown"


class EventType(str, Enum):
    """Types of company events"""
    FUNDING = "funding"
    PRODUCT_LAUNCH = "product_launch"
    PARTNERSHIP = "partnership"
    ACQUISITION = "acquisition"
    LEADERSHIP_CHANGE = "leadership_change"
    OFFICE_OPENING = "office_opening"
    MILESTONE = "milestone"
    OTHER = "other"


class FundingRound(BaseModel):
    """Funding round information"""
    date: Optional[str] = Field(None, description="Date of funding round (YYYY-MM-DD or YYYY-MM)")
    stage: Optional[FundingStage] = Field(None, description="Funding stage")
    amount: Optional[str] = Field(None, description="Amount raised (e.g., '$50M', 'Not disclosed')")
    amount_numeric: Optional[float] = Field(None, description="Numeric amount in millions USD")
    lead_investor: Optional[str] = Field(None, description="Lead investor name")
    other_investors: List[str] = Field(default_factory=list, description="Other participating investors")
    valuation: Optional[str] = Field(None, description="Company valuation (e.g., '$500M', 'Not disclosed')")
    source: Optional[str] = Field(None, description="Source of information")
    
    model_config = {"use_enum_values": True}  # ← UPDATED: Pydantic v2 style


class Event(BaseModel):
    """Company event (funding, partnership, product launch, etc.)"""
    date: Optional[str] = Field(None, description="Event date (YYYY-MM-DD or YYYY-MM)")
    event_type: EventType = Field(..., description="Type of event")
    title: str = Field(..., description="Event title/headline")
    description: Optional[str] = Field(None, description="Event description")
    source: Optional[str] = Field(None, description="Source URL or reference")
    
    model_config = {"use_enum_values": True}  # ← UPDATED: Pydantic v2 style


class LeadershipMember(BaseModel):
    """Company leadership member"""
    name: str = Field(..., description="Full name")
    title: str = Field(..., description="Job title")
    linkedin: Optional[str] = Field(None, description="LinkedIn profile URL")
    background: Optional[str] = Field(None, description="Brief background/bio")


class Product(BaseModel):
    """Company product or service"""
    name: str = Field(..., description="Product name")
    description: Optional[str] = Field(None, description="Product description")
    launch_date: Optional[str] = Field(None, description="Launch date if known")
    category: Optional[str] = Field(None, description="Product category")


class GrowthMetrics(BaseModel):
    """Growth and momentum indicators"""
    headcount: Optional[int] = Field(None, description="Current employee count")
    headcount_growth_yoy: Optional[float] = Field(None, description="Year-over-year headcount growth %")
    open_roles: Optional[int] = Field(None, description="Number of open job positions")
    recent_hires: Optional[int] = Field(None, description="Recent hires (last 6 months)")
    
    office_locations: List[str] = Field(default_factory=list, description="Office locations")
    geographic_expansion: Optional[str] = Field(None, description="Recent geographic expansion details")
    
    press_mentions_12m: Optional[int] = Field(None, description="Press mentions in last 12 months")
    website_traffic_trend: Optional[str] = Field(None, description="Website traffic trend")
    
    partnerships_count: Optional[int] = Field(None, description="Number of partnerships")
    recent_partnerships: List[str] = Field(default_factory=list, description="Recent partnership names")
    
    product_launches_12m: Optional[int] = Field(None, description="Product launches in last 12 months")
    recent_products: List[str] = Field(default_factory=list, description="Recent product names")
    
    customer_growth: Optional[str] = Field(None, description="Customer growth metric if disclosed")
    revenue_info: Optional[str] = Field(None, description="Revenue information if disclosed")


class Visibility(BaseModel):
    """Market visibility and sentiment indicators"""
    news_mentions_30d: Optional[int] = Field(None, description="News mentions in last 30 days")
    sentiment_score: Optional[float] = Field(None, description="Average sentiment score (0-1)")
    
    github_stars: Optional[int] = Field(None, description="GitHub stars if applicable")
    github_url: Optional[str] = Field(None, description="GitHub repository URL")
    
    glassdoor_rating: Optional[float] = Field(None, description="Glassdoor rating")
    glassdoor_reviews: Optional[int] = Field(None, description="Number of Glassdoor reviews")
    
    awards: List[str] = Field(default_factory=list, description="Recent awards or recognitions")
    media_coverage: List[str] = Field(default_factory=list, description="Notable media coverage")


class Company(BaseModel):
    """Core company information"""
    company_name: str = Field(..., description="Official company name")
    company_id: str = Field(..., description="Normalized company identifier")
    
    # Basic info
    website: str = Field(..., description="Company website URL")
    linkedin: Optional[str] = Field(None, description="LinkedIn company page URL")
    founded_year: Optional[int] = Field(None, description="Year founded")
    
    # Location
    hq_city: Optional[str] = Field(None, description="Headquarters city")
    hq_country: Optional[str] = Field(None, description="Headquarters country")
    
    # Category
    category: Optional[str] = Field(None, description="Business category/vertical")
    subcategory: Optional[str] = Field(None, description="Business subcategory")
    
    # Description
    tagline: Optional[str] = Field(None, description="Company tagline/one-liner")
    description: str = Field(..., description="Company description (2-3 sentences)")
    
    # Business model
    business_model: Optional[str] = Field(None, description="Business model (B2B, B2C, etc.)")
    target_customers: Optional[str] = Field(None, description="Target customer segment")
    pricing_model: Optional[str] = Field(None, description="Pricing model if known")
    
    # Competition
    competitors: List[str] = Field(default_factory=list, description="Known competitors")


class Snapshot(BaseModel):
    """Point-in-time snapshot of company state"""
    snapshot_date: str = Field(..., description="Date of this snapshot (YYYY-MM-DD)")
    
    # Funding
    total_funding: Optional[str] = Field(None, description="Total funding raised")
    total_funding_numeric: Optional[float] = Field(None, description="Total funding in millions USD")
    last_funding_date: Optional[str] = Field(None, description="Date of last funding round")
    last_funding_stage: Optional[FundingStage] = Field(None, description="Stage of last funding")
    valuation: Optional[str] = Field(None, description="Current valuation")
    
    # Team
    headcount: Optional[int] = Field(None, description="Current headcount")
    leadership_count: Optional[int] = Field(None, description="Number of leadership members")
    
    # Traction
    customer_count: Optional[str] = Field(None, description="Customer count if disclosed")
    revenue_range: Optional[str] = Field(None, description="Revenue range if disclosed")
    
    model_config = {"use_enum_values": True}  # ← UPDATED: Pydantic v2 style


class InvestorProfile(BaseModel):
    """Investor/funding profile"""
    total_raised: Optional[str] = Field(None, description="Total amount raised")
    total_raised_numeric: Optional[float] = Field(None, description="Total in millions USD")
    funding_rounds: List[FundingRound] = Field(default_factory=list, description="All funding rounds")
    lead_investors: List[str] = Field(default_factory=list, description="Lead investors across all rounds")
    all_investors: List[str] = Field(default_factory=list, description="All participating investors")
    last_round_date: Optional[str] = Field(None, description="Date of most recent round")
    estimated_runway: Optional[str] = Field(None, description="Estimated runway if calculable")


class DisclosureGaps(BaseModel):
    """Track what information is not disclosed"""
    missing_fields: List[str] = Field(default_factory=list, description="Fields marked as 'Not disclosed'")
    confidence_notes: Dict[str, str] = Field(default_factory=dict, description="Confidence level for uncertain data")


class CompanyPayload(BaseModel):
    """Complete payload for a company - used to generate dashboard"""
    # Core data
    company: Company
    snapshot: Snapshot
    investor_profile: InvestorProfile
    growth_metrics: GrowthMetrics
    visibility: Visibility
    
    # Time-series data
    events: List[Event] = Field(default_factory=list, description="Company events timeline")
    funding_rounds: List[FundingRound] = Field(default_factory=list, description="Funding history")
    leadership: List[LeadershipMember] = Field(default_factory=list, description="Leadership team")
    products: List[Product] = Field(default_factory=list, description="Product portfolio")
    
    # Metadata
    disclosure_gaps: DisclosureGaps = Field(default_factory=DisclosureGaps, description="Missing information")
    data_sources: List[str] = Field(default_factory=list, description="Data sources used")
    extracted_at: str = Field(..., description="Timestamp of data extraction")
    
    # Notes
    analyst_notes: Optional[str] = Field(None, description="Analyst notes or observations")
    risks: List[str] = Field(default_factory=list, description="Identified risks")
    opportunities: List[str] = Field(default_factory=list, description="Identified opportunities")


# Utility functions

def parse_funding_amount(amount_str: str) -> Optional[float]:
    """
    Parse funding amount string to numeric millions
    Examples: '$50M' -> 50.0, '$1.2B' -> 1200.0, 'Not disclosed' -> None
    """
    if not amount_str or 'not disclosed' in amount_str.lower():
        return None
    
    # Remove currency symbols and spaces
    amount_str = amount_str.replace('$', '').replace(',', '').strip()
    
    # Extract number and multiplier
    match = re.match(r'([\d.]+)\s*([MBK])?', amount_str, re.IGNORECASE)
    
    if not match:
        return None
    
    number = float(match.group(1))
    multiplier = match.group(2)
    
    if multiplier:
        multiplier = multiplier.upper()
        if multiplier == 'K':
            return number / 1000  # Convert to millions
        elif multiplier == 'M':
            return number
        elif multiplier == 'B':
            return number * 1000
    
    return number


def create_disclosure_gaps(payload: CompanyPayload) -> DisclosureGaps:
    """Automatically detect and populate disclosure gaps"""
    gaps = DisclosureGaps()
    
    # Check key fields
    if not payload.company.founded_year:
        gaps.missing_fields.append("Founded year")
    
    if not payload.snapshot.total_funding or 'not disclosed' in str(payload.snapshot.total_funding).lower():
        gaps.missing_fields.append("Total funding amount")
    
    if not payload.snapshot.valuation or 'not disclosed' in str(payload.snapshot.valuation).lower():
        gaps.missing_fields.append("Company valuation")
    
    if not payload.snapshot.headcount:
        gaps.missing_fields.append("Headcount")
    
    if not payload.growth_metrics.headcount_growth_yoy:
        gaps.missing_fields.append("Headcount growth rate")
    
    if not payload.snapshot.customer_count or 'not disclosed' in str(payload.snapshot.customer_count).lower():
        gaps.missing_fields.append("Customer count")
    
    if not payload.snapshot.revenue_range or 'not disclosed' in str(payload.snapshot.revenue_range).lower():
        gaps.missing_fields.append("Revenue")
    
    return gaps
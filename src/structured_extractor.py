"""
Lab 5 - Structured Extraction with Instructor
Extracts structured data from scraped text using LLM + Pydantic validation
"""

import instructor
from openai import OpenAI
from pathlib import Path
from typing import Optional
import os
from dotenv import load_dotenv

from src.models import (
    CompanyPayload,
    Company,
    Snapshot,
    InvestorProfile,
    GrowthMetrics,
    Visibility,
    FundingRound,
    Event,
    LeadershipMember,
    Product,
    DisclosureGaps,
    create_disclosure_gaps
)
from src.utils import (
    logger,
    load_json,
    save_json,
    normalize_company_name,
    ScraperConfig,
    get_timestamp
)

# Load environment variables
load_dotenv()


class StructuredExtractor:
    """Extract structured data using Instructor + OpenAI"""
    
    def __init__(self, model: str = "gpt-4o-mini"):
        """Initialize with OpenAI client"""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        # Patch OpenAI client with Instructor
        self.client = instructor.from_openai(OpenAI(api_key=api_key))
        self.model = model
        
        logger.info(f"Initialized StructuredExtractor with model: {model}")
    
    def load_scraped_text(self, run_folder: Path) -> dict:
        """Load all scraped text files - handles multiple folders per company"""
        
        texts = {}
        
        # Check if run_folder is actually a company folder with multiple timestamp subfolders
        # This handles the case where Lab1 scraper creates separate folders per page
        parent_folder = run_folder.parent
        
        # Collect all .txt files from ALL timestamp folders in initial/
        if parent_folder.name == "initial":
            logger.info(f"DEBUG: Multiple timestamp folders detected, merging content...")
            for timestamp_folder in parent_folder.iterdir():
                if timestamp_folder.is_dir():
                    for txt_file in timestamp_folder.glob("*.txt"):
                        page_type = txt_file.stem  # e.g., "homepage", "about", "careers"
                        
                        # Skip metadata and other non-content files
                        if 'metadata' in page_type or 'growth_momentum' in page_type:
                            continue
                        
                        try:
                            with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                                content = f.read()
                                if len(content) > 100:
                                    texts[page_type] = content[:15000]
                                    logger.info(f"DEBUG: Loaded {page_type}.txt from {timestamp_folder.name}")
                        except Exception as e:
                            logger.warning(f"Could not read {txt_file}: {e}")
        else:
            # Single folder structure - try standard file names
            text_files_new = {
                'homepage': run_folder / "homepage.txt",
                'about': run_folder / "about.txt",
                'careers': run_folder / "careers.txt",
                'products': run_folder / "products.txt",
                'blog': run_folder / "blog.txt"
            }
            
            text_files_old = {
                'homepage': run_folder / "homepage_clean.txt",
                'about': run_folder / "about_clean.txt",
                'careers': run_folder / "careers_clean.txt",
                'products': run_folder / "product_clean.txt",
                'blog': run_folder / "blog_clean.txt"
            }
            
            # Try new structure
            for page_type, file_path in text_files_new.items():
                if file_path.exists():
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            if len(content) > 100:
                                texts[page_type] = content[:15000]
                    except Exception as e:
                        logger.warning(f"Could not read {file_path}: {e}")
            
            # Try old structure if new didn't work
            if not texts:
                for page_type, file_path in text_files_old.items():
                    if file_path.exists():
                        try:
                            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                content = f.read()
                                if len(content) > 100:
                                    texts[page_type] = content[:15000]
                        except Exception as e:
                            logger.warning(f"Could not read {file_path}: {e}")
        
        return texts
    
    def load_growth_momentum(self, run_folder: Path) -> Optional[dict]:
        """Load pre-extracted growth momentum data"""
        growth_file = run_folder / "growth_momentum.json"
        if growth_file.exists():
            try:
                return load_json(growth_file)
            except:
                return None
        return None
    
    def load_metadata(self, run_folder: Path) -> Optional[dict]:
        """Load company metadata"""
        # Try to find any metadata file
        for meta_file in run_folder.glob("*_metadata.json"):
            try:
                return load_json(meta_file)
            except:
                continue
        return None
    
    def extract_company_info(self, company_name: str, texts: dict, metadata: dict) -> Company:
        """Extract core company information"""
        
        # Combine relevant text
        context = f"""
Company: {company_name}

HOMEPAGE:
{texts.get('homepage', '')[:5000]}

ABOUT PAGE:
{texts.get('about', '')[:5000]}
"""
        
        prompt = f"""Extract core company information from the provided text.

CRITICAL RULES:
- If information is not found, use "Not disclosed" for optional text fields
- If information is not found, use None for optional numeric fields
- Never invent or guess information
- Be conservative - only extract what is clearly stated

Extract:
- Official company name
- Website URL
- LinkedIn URL (if mentioned)
- Year founded (if mentioned)
- Headquarters city and country
- Business category/vertical
- Tagline or one-liner description
- Full company description (2-3 sentences)
- Business model (B2B, B2C, B2B2C, etc.)
- Target customer segment
- Known competitors (if mentioned)

Context:
{context}
"""
        
        try:
            company = self.client.chat.completions.create(
                model=self.model,
                response_model=Company,
                messages=[
                    {"role": "system", "content": "You are a business intelligence analyst extracting structured company data. Be accurate and conservative. Never invent information."},
                    {"role": "user", "content": prompt}
                ],
                max_retries=2
            )
            
            # Ensure required fields are set
            company.company_name = company_name
            company.company_id = normalize_company_name(company_name)
            if metadata and metadata.get('source_url'):
                company.website = metadata['source_url'].split('/')[0:3]
                company.website = '/'.join(company.website)
            
            return company
            
        except Exception as e:
            logger.error(f"Error extracting company info: {e}")
            # Return minimal valid company
            return Company(
                company_name=company_name,
                company_id=normalize_company_name(company_name),
                website=metadata.get('source_url', 'Not disclosed') if metadata else 'Not disclosed',
                description="Not disclosed"
            )
    
    def extract_funding_rounds(self, texts: dict) -> list[FundingRound]:
        """Extract funding round information"""
        
        context = f"""
HOMEPAGE:
{texts.get('homepage', '')[:3000]}

ABOUT PAGE:
{texts.get('about', '')[:3000]}

BLOG/NEWS:
{texts.get('blog', '')[:3000]}
"""
        
        prompt = f"""Extract all funding rounds mentioned in the text.

CRITICAL RULES:
- Only extract funding rounds that are explicitly mentioned
- If amount is not disclosed, set amount to "Not disclosed" and amount_numeric to None
- Use standard stage names: seed, series_a, series_b, series_c, series_d_plus
- Extract dates in YYYY-MM-DD or YYYY-MM format when possible
- If valuation is mentioned, include it
- List all investors mentioned

Extract ALL funding rounds found.

Context:
{context}
"""
        
        try:
            rounds = self.client.chat.completions.create(
                model=self.model,
                response_model=list[FundingRound],
                messages=[
                    {"role": "system", "content": "You are a financial analyst extracting funding round data. Only extract explicitly stated information."},
                    {"role": "user", "content": prompt}
                ],
                max_retries=2
            )
            return rounds or []
        except Exception as e:
            logger.error(f"Error extracting funding rounds: {e}")
            return []
    
    def extract_growth_metrics(self, texts: dict, growth_momentum: Optional[dict]) -> GrowthMetrics:
        """Extract growth and momentum indicators"""
        
        context = f"""
ABOUT PAGE:
{texts.get('about', '')[:3000]}

CAREERS PAGE:
{texts.get('careers', '')[:2000]}

BLOG/NEWS:
{texts.get('blog', '')[:2000]}

PRE-EXTRACTED DATA:
{growth_momentum if growth_momentum else 'None available'}
"""
        
        prompt = f"""Extract growth metrics and momentum indicators.

CRITICAL RULES:
- Use "Not disclosed" for unavailable information
- For numeric fields, use None if not available
- Extract open roles from careers page
- Look for headcount, team size, or employee count mentions
- Find office locations mentioned
- Identify partnerships announced
- Note recent product launches

Context:
{context}
"""
        
        try:
            metrics = self.client.chat.completions.create(
                model=self.model,
                response_model=GrowthMetrics,
                messages=[
                    {"role": "system", "content": "You are a growth analyst extracting momentum indicators. Be conservative and accurate."},
                    {"role": "user", "content": prompt}
                ],
                max_retries=2
            )
            
            # Supplement with pre-extracted data if available
            if growth_momentum:
                if not metrics.open_roles and growth_momentum.get('open_roles', {}).get('count'):
                    metrics.open_roles = growth_momentum['open_roles']['count']
                if not metrics.office_locations and growth_momentum.get('office_locations'):
                    metrics.office_locations = growth_momentum['office_locations'][:5]
            
            return metrics
        except Exception as e:
            logger.error(f"Error extracting growth metrics: {e}")
            return GrowthMetrics()
    
    def extract_events(self, texts: dict) -> list[Event]:
        """Extract company events (funding, launches, partnerships)"""
        
        context = f"""
BLOG/NEWS:
{texts.get('blog', '')[:5000]}

ABOUT PAGE:
{texts.get('about', '')[:2000]}
"""
        
        prompt = f"""Extract major company events from the text.

Look for:
- Funding announcements
- Product launches
- Partnerships or collaborations
- Acquisitions
- Leadership changes
- Office openings
- Major milestones

CRITICAL RULES:
- Only extract events explicitly mentioned
- Include dates when available (YYYY-MM-DD format preferred)
- Provide brief descriptions
- Categorize event type accurately

Extract up to 10 most important events.

Context:
{context}
"""
        
        try:
            events = self.client.chat.completions.create(
                model=self.model,
                response_model=list[Event],
                messages=[
                    {"role": "system", "content": "You are an analyst extracting company timeline events. Only extract explicitly mentioned events."},
                    {"role": "user", "content": prompt}
                ],
                max_retries=2
            )
            return events[:10] if events else []
        except Exception as e:
            logger.error(f"Error extracting events: {e}")
            return []
    
    def create_investor_profile(self, funding_rounds: list[FundingRound]) -> InvestorProfile:
        """Create investor profile from funding rounds"""
        if not funding_rounds:
            return InvestorProfile()
        
        # Calculate total raised
        total_numeric = sum(r.amount_numeric for r in funding_rounds if r.amount_numeric)
        
        # Get all investors
        lead_investors = [r.lead_investor for r in funding_rounds if r.lead_investor]
        all_investors = lead_investors.copy()
        for r in funding_rounds:
            all_investors.extend(r.other_investors)
        all_investors = list(set(all_investors))  # Remove duplicates
        
        # Get last round date
        dates = [r.date for r in funding_rounds if r.date]
        last_date = max(dates) if dates else None
        
        return InvestorProfile(
            total_raised=f"${total_numeric}M" if total_numeric else "Not disclosed",
            total_raised_numeric=total_numeric if total_numeric else None,
            funding_rounds=funding_rounds,
            lead_investors=lead_investors,
            all_investors=all_investors,
            last_round_date=last_date
        )
    
    def create_snapshot(self, funding_rounds: list[FundingRound], growth_metrics: GrowthMetrics) -> Snapshot:
        """Create point-in-time snapshot"""
        
        # Get last funding info
        last_round = funding_rounds[-1] if funding_rounds else None
        
        return Snapshot(
            snapshot_date=get_timestamp().split('T')[0],  # Just date
            total_funding=f"${sum(r.amount_numeric for r in funding_rounds if r.amount_numeric)}M" if funding_rounds else "Not disclosed",
            total_funding_numeric=sum(r.amount_numeric for r in funding_rounds if r.amount_numeric) if funding_rounds else None,
            last_funding_date=last_round.date if last_round else None,
            last_funding_stage=last_round.stage if last_round else None,
            valuation=last_round.valuation if last_round else "Not disclosed",
            headcount=growth_metrics.headcount
        )
    
    def extract_company(self, company_name: str, run_folder: Path) -> Optional[CompanyPayload]:
        """Extract complete structured data for one company"""
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Extracting structured data for: {company_name}")
        logger.info(f"{'='*60}\n")
        
        # DEBUG: Show what folder we're using
        logger.info(f"DEBUG: Using run folder: {run_folder}")
        logger.info(f"DEBUG: Folder exists: {run_folder.exists()}")
        if run_folder.exists():
            logger.info(f"DEBUG: Files in folder: {list(run_folder.iterdir())[:10]}")
        
        try:
            # Load all available data
            texts = self.load_scraped_text(run_folder)
            growth_momentum = self.load_growth_momentum(run_folder)
            metadata = self.load_metadata(run_folder)
            
            # DEBUG: Show what we loaded
            logger.info(f"DEBUG: Text files loaded: {list(texts.keys())}")
            logger.info(f"DEBUG: Growth momentum: {'Found' if growth_momentum else 'Not found'}")
            logger.info(f"DEBUG: Metadata: {'Found' if metadata else 'Not found'}")
            
            if not texts:
                logger.error(f"No text content found for {company_name}")
                return None
            
            logger.info(f"Loaded {len(texts)} text files")
            
            # Extract each component
            logger.info("Extracting company info...")
            company = self.extract_company_info(company_name, texts, metadata)
            
            logger.info("Extracting funding rounds...")
            funding_rounds = self.extract_funding_rounds(texts)
            
            logger.info("Extracting growth metrics...")
            growth_metrics = self.extract_growth_metrics(texts, growth_momentum)
            
            logger.info("Extracting events...")
            events = self.extract_events(texts)
            
            # Create derived components
            investor_profile = self.create_investor_profile(funding_rounds)
            snapshot = self.create_snapshot(funding_rounds, growth_metrics)
            
            # Create payload
            payload = CompanyPayload(
                company=company,
                snapshot=snapshot,
                investor_profile=investor_profile,
                growth_metrics=growth_metrics,
                visibility=Visibility(),  # Placeholder - would need external APIs
                events=events,
                funding_rounds=funding_rounds,
                leadership=[],  # Would need LinkedIn scraping
                products=[],  # Could extract from product page
                disclosure_gaps=DisclosureGaps(),  # Will populate below
                data_sources=[f"Website: {company.website}"],
                extracted_at=get_timestamp()
            )
            
            # Auto-detect disclosure gaps
            payload.disclosure_gaps = create_disclosure_gaps(payload)
            
            logger.info(f"✓ Extraction complete")
            logger.info(f"  - Funding rounds: {len(funding_rounds)}")
            logger.info(f"  - Events: {len(events)}")
            logger.info(f"  - Disclosure gaps: {len(payload.disclosure_gaps.missing_fields)}")
            
            return payload
            
        except Exception as e:
            logger.error(f"Error extracting {company_name}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def extract_all_companies(self, output_dir: Path = None):
        """Extract structured data for all scraped companies"""
        
        if output_dir is None:
            output_dir = ScraperConfig.DATA_DIR / "structured"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        raw_dir = ScraperConfig.RAW_DIR
        
        if not raw_dir.exists():
            logger.error(f"Raw data directory not found: {raw_dir}")
            return
        
        results = []
        successful = 0
        failed = 0
        
        # Find all company folders
        company_folders = sorted([f for f in raw_dir.iterdir() if f.is_dir()])
        total = len(company_folders)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"STARTING STRUCTURED EXTRACTION")
        logger.info(f"Total companies: {total}")
        logger.info(f"{'#'*60}\n")
        
        for idx, company_folder in enumerate(company_folders, 1):
            company_id = company_folder.name
            
            logger.info(f"\n[{idx}/{total}] Processing: {company_id}")
            
            # Try NEW structure first (initial/ folder)
            initial_folder = company_folder / "initial"
            run_folder = None
            
            if initial_folder.exists():
                run_folders = sorted(initial_folder.iterdir(), reverse=True)
                if run_folders:
                    run_folder = run_folders[0]
            
            # Fallback to OLD structure (full_* folders)
            if not run_folder:
                run_folders = sorted(company_folder.glob("full_*"), reverse=True)
                if run_folders:
                    run_folder = run_folders[0]
            
            if not run_folder:
                logger.warning(f"No data folder found for {company_id}")
                failed += 1
                continue
            
            # Extract
            payload = self.extract_company(company_id, run_folder)
            
            if payload:
                # Save to structured folder
                output_file = output_dir / f"{company_id}.json"
                
                # Convert to dict for saving
                payload_dict = payload.model_dump(mode='json')
                save_json(payload_dict, output_file)
                
                results.append({
                    'company_id': company_id,
                    'company_name': payload.company.company_name,
                    'status': 'success',
                    'output_file': str(output_file)
                })
                successful += 1
            else:
                results.append({
                    'company_id': company_id,
                    'status': 'failed'
                })
                failed += 1
        
        # Save summary report
        report = {
            'extracted_at': get_timestamp(),
            'total_companies': total,
            'successful': successful,
            'failed': failed,
            'output_directory': str(output_dir),
            'results': results
        }
        
        report_file = ScraperConfig.DATA_DIR / "structured_extraction_report.json"
        save_json(report, report_file)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"EXTRACTION COMPLETE")
        logger.info(f"{'#'*60}")
        logger.info(f"Total: {total}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Success rate: {successful/total*100:.1f}%")
        logger.info(f"Output: {output_dir}")
        logger.info(f"Report: {report_file}")
        logger.info(f"{'#'*60}\n")


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Lab 5 - Structured Extraction with Instructor")
    parser.add_argument('--company-id', type=str, help='Extract specific company only')
    parser.add_argument('--model', type=str, default='gpt-4o-mini', help='OpenAI model to use')
    parser.add_argument('--output-dir', type=str, help='Output directory for structured data')
    
    args = parser.parse_args()
    
    extractor = StructuredExtractor(model=args.model)
    
    if args.company_id:
        # Extract single company
        company_folder = ScraperConfig.RAW_DIR / args.company_id
        
        # Try NEW structure first (initial/ folder)
        initial_folder = company_folder / "initial"
        run_folder = None
        
        if initial_folder.exists():
            run_folders = sorted(initial_folder.iterdir(), reverse=True)
            if run_folders:
                run_folder = run_folders[0]
        
        # Fallback to OLD structure (full_* folders)
        if not run_folder:
            run_folders = sorted(company_folder.glob("full_*"), reverse=True)
            if run_folders:
                run_folder = run_folders[0]
        
        if not run_folder:
            logger.error(f"No data found for {args.company_id}")
        else:
            logger.info(f"Using data from: {run_folder}")
            payload = extractor.extract_company(args.company_id, run_folder)
            
            if payload:
                output_dir = Path(args.output_dir) if args.output_dir else ScraperConfig.DATA_DIR / "structured"
                output_dir.mkdir(parents=True, exist_ok=True)
                
                output_file = output_dir / f"{args.company_id}.json"
                payload_dict = payload.model_dump(mode='json')
                save_json(payload_dict, output_file)
                
                logger.info(f"\n✓ Saved to: {output_file}")
    else:
        # Extract all companies
        output_dir = Path(args.output_dir) if args.output_dir else None
        extractor.extract_all_companies(output_dir)
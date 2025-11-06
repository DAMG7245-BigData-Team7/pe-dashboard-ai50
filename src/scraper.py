import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from dataclasses import dataclass
import re

from src.utils import (
    ScraperConfig, 
    logger, 
    compute_hash, 
    save_json,
    normalize_company_name,
    get_timestamp,
    create_run_folder,
    ensure_directory,
    validate_company_data,
    load_and_validate_companies
)


@dataclass
class ScrapedPage:
    """Container for scraped page data"""
    url: str
    raw_html: str
    clean_text: str
    metadata: Dict
    success: bool
    error: Optional[str] = None


class CompanyScraper:
    """Scraper for individual company websites"""
    
    def __init__(self, company_name: str, base_url: str):
        self.company_name = company_name
        self.company_id = normalize_company_name(company_name)
        self.base_url = base_url.rstrip('/')
        self.domain = urlparse(base_url).netloc
        self.session = requests.Session()
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str) -> Tuple[str, int]:
        """
        Fetch a single page with retry logic
        Returns: (html_content, status_code)
        """
        try:
            ScraperConfig.random_delay()  # Human-like behavior
            
            response = self.session.get(
                url,
                headers=ScraperConfig.get_headers(),
                timeout=ScraperConfig.REQUEST_TIMEOUT,
                allow_redirects=True
            )
            response.raise_for_status()
            
            logger.info(f"✓ Fetched {url} (Status: {response.status_code})")
            return response.text, response.status_code
            
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Error fetching {url}: {str(e)}")
            raise
    
    def clean_html(self, html: str) -> str:
        """
        Extract clean text from HTML
        Remove scripts, styles, navigation, footers
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Get text
        text = soup.get_text(separator='\n', strip=True)
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract metadata from page"""
        metadata = {
            'title': '',
            'description': '',
            'keywords': [],
            'headings': []
        }
        
        # Title
        if soup.title:
            metadata['title'] = soup.title.string.strip() if soup.title.string else ''
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            metadata['description'] = meta_desc['content'].strip()
        
        # Meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            metadata['keywords'] = [k.strip() for k in meta_keywords['content'].split(',')]
        
        # Extract all headings
        for level in ['h1', 'h2', 'h3']:
            headings = soup.find_all(level)
            metadata['headings'].extend([h.get_text(strip=True) for h in headings])
        
        return metadata
    
    def scrape_page(self, url: str, page_type: str) -> ScrapedPage:
        """
        Scrape a single page and return structured data
        """
        try:
            html, status_code = self.fetch_page(url)
            soup = BeautifulSoup(html, 'lxml')
            
            clean_text = self.clean_html(html)
            page_metadata = self.extract_metadata(soup, url)
            
            metadata = {
                'company_name': self.company_name,
                'company_id': self.company_id,
                'page_type': page_type,
                'source_url': url,
                'crawled_at': get_timestamp(),
                'status_code': status_code,
                'content_hash': compute_hash(html),
                'text_length': len(clean_text),
                **page_metadata
            }
            
            return ScrapedPage(
                url=url,
                raw_html=html,
                clean_text=clean_text,
                metadata=metadata,
                success=True
            )
            
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {str(e)}")
            return ScrapedPage(
                url=url,
                raw_html='',
                clean_text='',
                metadata={
                    'company_name': self.company_name,
                    'company_id': self.company_id,
                    'page_type': page_type,
                    'source_url': url,
                    'crawled_at': get_timestamp(),
                    'error': str(e)
                },
                success=False,
                error=str(e)
            )
    
    def find_page_url(self, page_type: str) -> Optional[str]:
        """
        Try to find the URL for a specific page type
        Common patterns for different page types
        """
        patterns = {
            'about': ['/about', '/about-us', '/company', '/who-we-are', '/about-us/', '/company/'],
            'careers': ['/careers', '/jobs', '/join-us', '/work-with-us', '/opportunities', '/careers/', '/jobs/'],
            'product': ['/product', '/products', '/platform', '/solutions', '/technology', '/products/', '/platform/'],
            'blog': ['/blog', '/news', '/insights', '/resources', '/updates', '/blog/', '/news/'],
            'press': ['/press', '/newsroom', '/media', '/press-releases', '/press/', '/newsroom/']
        }
        
        if page_type not in patterns:
            return None
        
        # Try each pattern
        for pattern in patterns[page_type]:
            potential_url = urljoin(self.base_url, pattern)
            try:
                response = self.session.head(
                    potential_url, 
                    headers=ScraperConfig.get_headers(),
                    timeout=10,
                    allow_redirects=True
                )
                if response.status_code == 200:
                    logger.info(f"Found {page_type} page: {potential_url}")
                    return potential_url
            except:
                continue
        
        logger.warning(f"Could not find {page_type} page for {self.company_name}")
        return None
    
    def scrape_all_pages(self, run_folder: Path) -> Dict:
        """
        Scrape all important pages for the company
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting scrape for: {self.company_name}")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Run folder: {run_folder}")
        logger.info(f"{'='*60}\n")
        
        results = {
            'company_name': self.company_name,
            'company_id': self.company_id,
            'base_url': self.base_url,
            'scraped_at': get_timestamp(),
            'pages': {},
            'summary': {
                'total_pages': 0,
                'successful': 0,
                'failed': 0
            }
        }
        
        # Page types to scrape
        page_types = {
            'homepage': self.base_url,
            'about': None,
            'careers': None,
            'product': None,
            'blog': None
        }
        
        # Find URLs for each page type
        for page_type in ['about', 'careers', 'product', 'blog']:
            page_types[page_type] = self.find_page_url(page_type)
        
        # Scrape each page
        for page_type, url in page_types.items():
            if url is None:
                logger.warning(f"Skipping {page_type} - URL not found")
                results['pages'][page_type] = {
                    'success': False,
                    'error': 'URL not found'
                }
                results['summary']['failed'] += 1
                continue
            
            scraped = self.scrape_page(url, page_type)
            
            if scraped.success:
                # Save raw HTML
                html_file = run_folder / f"{page_type}_raw.html"
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(scraped.raw_html)
                
                # Save clean text
                text_file = run_folder / f"{page_type}_clean.txt"
                with open(text_file, 'w', encoding='utf-8') as f:
                    f.write(scraped.clean_text)
                
                # Save metadata
                meta_file = run_folder / f"{page_type}_meta.json"
                save_json(scraped.metadata, meta_file)
                
                results['pages'][page_type] = {
                    'success': True,
                    'url': scraped.url,
                    'files': {
                        'html': str(html_file),
                        'text': str(text_file),
                        'metadata': str(meta_file)
                    },
                    'metadata': scraped.metadata
                }
                results['summary']['successful'] += 1
            else:
                results['pages'][page_type] = {
                    'success': False,
                    'error': scraped.error
                }
                results['summary']['failed'] += 1
            
            results['summary']['total_pages'] += 1
        
        # Save overall summary
        summary_file = run_folder / "scrape_summary.json"
        save_json(results, summary_file)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Scrape completed for: {self.company_name}")
        logger.info(f"Successful: {results['summary']['successful']}/{results['summary']['total_pages']}")
        logger.info(f"{'='*60}\n")
        
        return results


class ForbesAI50Scraper:
    """Main scraper orchestrator for all Forbes AI 50 companies"""
    
    def __init__(self, companies_file: Path, skip_invalid: bool = True):
        self.companies_file = companies_file
        self.skip_invalid = skip_invalid
        self.companies, self.invalid_companies = self._load_and_validate()
        
    def _load_and_validate(self) -> Tuple[List[Dict], List[Dict]]:
        """Load and validate company data"""
        valid, invalid = load_and_validate_companies(self.companies_file)
        
        if invalid:
            logger.warning(f"\n{'!'*60}")
            logger.warning(f"Found {len(invalid)} invalid companies:")
            for inv in invalid:
                logger.warning(f"  - {inv.get('company_name', 'Unknown')}: {inv.get('validation_error')}")
            logger.warning(f"{'!'*60}\n")
            
            # Save invalid companies for review
            invalid_file = ScraperConfig.DATA_DIR / "invalid_companies.json"
            save_json(invalid, invalid_file)
            logger.info(f"Saved invalid companies to {invalid_file}")
        
        return valid, invalid
    
    def scrape_company(self, company: Dict, run_type: str = "full") -> Dict:
        """Scrape a single company"""
        company_name = company['company_name']
        website = company.get('website')
        
        if not website or 'example.com' in website:
            logger.warning(f"Skipping {company_name} - placeholder or invalid website")
            return {
                'company_name': company_name,
                'company_id': normalize_company_name(company_name),
                'success': False,
                'error': 'Invalid or placeholder website'
            }
        
        # Store additional metadata from JSON
        company_metadata = {
            'linkedin': company.get('linkedin'),
            'hq_city': company.get('hq_city'),
            'hq_country': company.get('hq_country'),
            'category': company.get('category')
        }
        
        # Create run folder
        company_id = normalize_company_name(company_name)
        run_folder = create_run_folder(company_id, run_type)
        
        # Save company metadata to run folder
        company_meta_file = run_folder / "company_info.json"
        save_json({
            'company_name': company_name,
            'company_id': company_id,
            'website': website,
            **company_metadata,
            'scraped_at': get_timestamp()
        }, company_meta_file)
        
        # Scrape
        scraper = CompanyScraper(company_name, website)
        results = scraper.scrape_all_pages(run_folder)
        
        # Add company metadata to results
        results['company_metadata'] = company_metadata
        
        return results
    
    def scrape_all(self, run_type: str = "full", limit: Optional[int] = None):
        """
        Scrape all companies (or limited number for testing)
        """
        companies_to_scrape = self.companies[:limit] if limit else self.companies
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"STARTING SCRAPE FOR {len(companies_to_scrape)} COMPANIES")
        logger.info(f"Run type: {run_type}")
        if self.invalid_companies:
            logger.info(f"Skipped {len(self.invalid_companies)} invalid companies")
        logger.info(f"{'#'*60}\n")
        
        all_results = []
        successful = 0
        failed = 0
        
        for idx, company in enumerate(companies_to_scrape, 1):
            logger.info(f"\n[{idx}/{len(companies_to_scrape)}] Processing {company['company_name']}")
            
            result = self.scrape_company(company, run_type)
            all_results.append(result)
            
            # Track success/failure
            if result.get('summary', {}).get('successful', 0) > 0:
                successful += 1
            else:
                failed += 1
            
            # Save progress after each company
            progress_file = ScraperConfig.DATA_DIR / f"scrape_progress_{run_type}.json"
            save_json({
                'run_type': run_type,
                'timestamp': get_timestamp(),
                'total_companies': len(companies_to_scrape),
                'completed': idx,
                'successful': successful,
                'failed': failed,
                'invalid_skipped': len(self.invalid_companies),
                'results': all_results
            }, progress_file)
        
        # Final summary
        logger.info(f"\n{'#'*60}")
        logger.info(f"SCRAPING COMPLETE!")
        logger.info(f"Total companies processed: {len(all_results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Invalid (skipped): {len(self.invalid_companies)}")
        logger.info(f"{'#'*60}\n")
        
        # Save final report
        from datetime import datetime
        final_report = ScraperConfig.DATA_DIR / f"scrape_report_{run_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_json({
            'run_type': run_type,
            'timestamp': get_timestamp(),
            'summary': {
                'total_attempted': len(companies_to_scrape),
                'successful': successful,
                'failed': failed,
                'invalid_skipped': len(self.invalid_companies)
            },
            'results': all_results,
            'invalid_companies': self.invalid_companies
        }, final_report)
        
        logger.info(f"Final report saved to: {final_report}")
        
        return all_results


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape Forbes AI 50 companies")
    parser.add_argument('--companies', type=str, default='data/forbes_ai50_seed.json',
                       help='Path to companies JSON file')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of companies (for testing)')
    parser.add_argument('--run-type', type=str, default='full',
                       choices=['full', 'daily'],
                       help='Type of scrape run')
    
    args = parser.parse_args()
    
    # Run scraper
    scraper = ForbesAI50Scraper(Path(args.companies))
    scraper.scrape_all(run_type=args.run_type, limit=args.limit)
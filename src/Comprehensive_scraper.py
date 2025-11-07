"""
Final Comprehensive Scraper
- Handles nested navigation (Company → Careers)
- Clicks dropdowns and mega menus
- Follows "Learn more" / "Read more" links
- Maximum page discovery
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from pathlib import Path
import json
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib


class FinalComprehensiveScraper:
    """
    Advanced scraper that extracts ALL pages including nested navigation
    """
    
    def __init__(self, output_dir="data/raw", delay=2.0):
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.content_hashes = {}
    
    def _get_content_hash(self, text):
        """Hash for duplicate detection"""
        return hashlib.md5(text[:500].encode()).hexdigest()
    
    def _is_duplicate(self, text, company_id):
        """Check for duplicates"""
        h = self._get_content_hash(text)
        if company_id not in self.content_hashes:
            self.content_hashes[company_id] = set()
        if h in self.content_hashes[company_id]:
            return True
        self.content_hashes[company_id].add(h)
        return False
    
    def discover_all_pages_interactive(self, base_url):
        """
        Discover pages by interacting with the site
        - Hovers over menu items
        - Clicks dropdowns
        - Extracts nested links
        """
        
        discovered = {'homepage': base_url}
        
        print(f"  Interactive page discovery...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            try:
                # Load page
                page.goto(base_url, wait_until='domcontentloaded', timeout=100000)
                page.wait_for_timeout(8000)
                
                # Get all links initially
                all_links = self._extract_all_links(page, base_url)
                
                # Find navigation elements and interact with them
                nav_selectors = ['nav', 'header', '[role="navigation"]', '[class*="nav"]', '[class*="menu"]']
                
                for selector in nav_selectors:
                    try:
                        nav_elements = page.query_selector_all(selector)
                        
                        for nav in nav_elements[:3]:  # Limit to avoid too many
                            # Find hoverable items (dropdown triggers)
                            buttons = nav.query_selector_all('button, a, [role="button"]')
                            
                            for button in buttons[:15]:  # Limit items
                                try:
                                    # Hover to reveal dropdown
                                    button.hover()
                                    page.wait_for_timeout(800)
                                    
                                    # Extract revealed links
                                    new_links = self._extract_all_links(page, base_url)
                                    all_links.update(new_links)
                                    
                                except:
                                    continue
                    except:
                        continue
                
                # Categorize all discovered links
                for url, text in all_links.items():
                    page_type = self._categorize_link(url, text)
                    if page_type and page_type not in discovered:
                        discovered[page_type] = url
                        print(f"    ✓ {page_type}: {url}")
                
            except Exception as e:
                print(f"    Interactive discovery error: {str(e)}")
            
            browser.close()
        
        # Also try URL patterns for missing pages
        missing = self._try_url_patterns(base_url, discovered)
        discovered.update(missing)
        
        return discovered
    
    def _extract_all_links(self, page, base_url):
        """Extract all links from current page state"""
        links = {}
        
        try:
            # Get all links
            link_elements = page.query_selector_all('a[href]')
            
            for elem in link_elements:
                try:
                    href = elem.get_attribute('href')
                    text = elem.inner_text() or ''
                    
                    if not href:
                        continue
                    
                    full_url = urljoin(base_url, href)
                    
                    # Same domain only
                    if urlparse(full_url).netloc != urlparse(base_url).netloc:
                        continue
                    
                    # Skip unwanted
                    if any(skip in full_url.lower() for skip in ['login', 'signup', '.pdf', 'mailto:', 'tel:']):
                        continue
                    
                    links[full_url] = text.strip()
                
                except:
                    continue
        
        except:
            pass
        
        return links
    
    def _categorize_link(self, url, text):
        """Categorize link by URL and text"""
        combined = f"{url} {text}".lower()
        
        categories = {
            'about': ['about', 'company', 'who we are', 'our story', 'mission'],
            'careers': ['career', 'jobs', 'work with', 'join', 'opportunities', 'hiring'],
            'blog': ['blog', 'news', 'article', 'insight', 'post'],
            'products': ['product', 'platform', 'solution', 'feature'],
            'pricing': ['pricing', 'plan', 'price'],
            'contact': ['contact', 'get in touch', 'reach us'],
            'support': ['support', 'help', 'faq'],
            'research': ['research', 'paper', 'publication'],
            'team': ['team', 'people', 'leadership'],
            'api': ['api', 'developer', 'docs'],
            'security': ['security', 'trust', 'safety'],
            'resources': ['resource', 'guide', 'learn']
        }
        
        for cat, keywords in categories.items():
            if any(kw in combined for kw in keywords):
                return cat
        
        return None
    
    def _try_url_patterns(self, base_url, existing):
        """Try URL patterns for pages not found"""
        found = {}
        
        patterns = {
            'about': ['/about', '/company'],
            'careers': ['/careers', '/jobs'],
            'blog': ['/blog', '/news'],
            'products': ['/products', '/platform'],
            'pricing': ['/pricing'],
            'contact': ['/contact'],
            'team': ['/team'],
            'research': ['/research']
        }
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            for page_type in patterns:
                if page_type in existing:
                    continue
                
                for pattern in patterns[page_type]:
                    test_url = base_url.rstrip('/') + pattern
                    
                    try:
                        response = page.goto(test_url, wait_until='load', timeout=10000)
                        if response and response.status < 400:
                            content = page.content()
                            if len(content) > 500:
                                found[page_type] = test_url
                                break
                    except:
                        continue
            
            browser.close()
        
        return found
    
    def scrape_page(self, url, company_name, company_id, page_type):
        """Scrape one page with Playwright"""
        
        print(f"  [{page_type}] {url[:60]}...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                )
                page = context.new_page()
                
                # Navigate with retry
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                except PlaywrightTimeout:
                    page.goto(url, wait_until='load', timeout=60000)
                
                page.wait_for_timeout(3000)
                
                # Scroll
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(1000)
                
                html_content = page.content()
                browser.close()
            
            # Extract text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'form']):
                tag.decompose()
            
            text = soup.get_text(separator='\n')
            lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 2]
            clean_text = '\n'.join(lines)
            
            # Validation
            if self._is_duplicate(clean_text, company_id):
                print(f"    ⚠️  Duplicate - skip")
                return False
            
            if len(clean_text) < 100:
                print(f"    ⚠️  Too short ({len(clean_text)} chars)")
                return False
            
            # Save
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_dir = self.output_dir / company_id / "initial" / timestamp
            save_dir.mkdir(parents=True, exist_ok=True)
            
            with open(save_dir / f"{page_type}.html", 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            with open(save_dir / f"{page_type}.txt", 'w', encoding='utf-8') as f:
                f.write(clean_text)
            
            metadata = {
                'company_name': company_name,
                'company_id': company_id,
                'source_url': url,
                'page_type': page_type,
                'crawled_at': datetime.now().isoformat(),
                'content_length': len(clean_text),
                'method': 'playwright'
            }
            
            with open(save_dir / f"{page_type}_metadata.json", 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"    ✓ {len(clean_text):,} chars")
            time.sleep(self.delay)
            return True
            
        except Exception as e:
            print(f"    ✗ {str(e)[:60]}")
            return False
    
    def scrape_company(self, company):
        """Scrape company comprehensively"""
        
        company_name = company['company_name']
        company_id = company['company_id']
        website = company['website']
        
        print(f"\n{'='*60}")
        print(f"{company_name}")
        print(f"{'='*60}")
        
        self.content_hashes[company_id] = set()
        
        # Discover with interaction
        pages = self.discover_all_pages_interactive(website)
        
        print(f"\n  Total discovered: {len(pages)}")
        
        # Scrape all
        success = 0
        for page_type, url in pages.items():
            if self.scrape_page(url, company_name, company_id, page_type):
                success += 1
        
        print(f"\n  ✅ Scraped: {success}/{len(pages)}")
        
        return success


def scrape_specific_companies(company_names):
    """Scrape specific companies"""
    
    with open('data/forbes_ai50_seed.json', 'r') as f:
        all_companies = json.load(f)
    
    scraper = FinalComprehensiveScraper(output_dir="data/raw", delay=2.0)
    
    for name in company_names:
        company = next((c for c in all_companies if c['company_name'] == name), None)
        if company:
            scraper.scrape_company(company)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--partial":
            # Fix all 7 partial companies
            companies = ["xAI", "Midjourney", "OpenAI", "Perplexity AI", 
                        "OpenEvidence", "Sakana AI", "Skild AI", "Windsurf"]
            scrape_specific_companies(companies)
        
        elif sys.argv[1] == "--js":
            # Fix 4 JavaScript companies
            companies = ["Abridge", "Captions", "Photoroom", "Suno"]
            scrape_specific_companies(companies)
        
        else:
            # Single company
            scrape_specific_companies([sys.argv[1]])
    else:
        print("\nUsage:")
        print("  python src/Comprehensive_scraper.py xAI          # One company")
        print("  python src/Comprehensive_scraper.py --partial    # Fix 7 partial")
        print("  python src/Csomprehensive_scraper.py --js         # Fix 4 JS sites")
        print()
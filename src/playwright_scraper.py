"""
Playwright-based Scraper
Handles JavaScript sites and anti-bot protection better than Selenium
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from pathlib import Path
import json
import time
from datetime import datetime
from urllib.parse import urljoin
import hashlib


class PlaywrightScraper:
    """Advanced scraper using Playwright for difficult sites"""
    
    def __init__(self, output_dir="data/raw", delay=2.0):
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.content_hashes = {}
    
    def _get_content_hash(self, text):
        """Get hash for duplicate detection"""
        sample = text[:500].strip()
        return hashlib.md5(sample.encode()).hexdigest()
    
    def _is_duplicate_content(self, text, company_id):
        """Check for duplicate content"""
        content_hash = self._get_content_hash(text)
        
        if company_id not in self.content_hashes:
            self.content_hashes[company_id] = set()
        
        if content_hash in self.content_hashes[company_id]:
            return True
        
        self.content_hashes[company_id].add(content_hash)
        return False
    
    def scrape_page_with_playwright(self, url, company_name, company_id, page_type):
        """Scrape page using Playwright (handles JavaScript)"""
        
        print(f"  Scraping {page_type}: {url[:60]}...")
        
        try:
            with sync_playwright() as p:
                # Launch browser with stealth settings
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox'
                    ]
                )
                
                # Create context with real browser-like settings
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                
                page = context.new_page()
                
                # Go to URL with longer timeout and different wait strategy
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)  # 60 sec timeout
                    page.wait_for_timeout(3000)  # Wait for JS to execute
                except PlaywrightTimeout:
                    # If networkidle fails, try with just load
                    print(f"      Retrying with simpler wait strategy...")
                    page.goto(url, wait_until='load', timeout=60000)
                
                # Wait for content to load
                page.wait_for_timeout(3000)
                
                # Scroll to trigger lazy loading
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(2000)
                
                # Get content
                html_content = page.content()
                
                browser.close()
            
            # Extract text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove junk
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'form', 'button']):
                tag.decompose()
            
            # Get text
            text = soup.get_text(separator='\n')
            
            # Clean it
            lines = [line.strip() for line in text.split('\n') if len(line.strip()) > 2]
            clean_text = '\n'.join(lines)
            
            # Check if still has JavaScript warning
            if 'enable JavaScript' in clean_text[:500]:
                print(f"    ⚠️  Still requires JavaScript (site may be blocking)")
                # Continue anyway - might have some content
            
            # Check for duplicate
            if self._is_duplicate_content(clean_text, company_id):
                print(f"    ⚠️  Duplicate content - skipping")
                return False
            
            # Check substantial content
            if len(clean_text) < 100:
                print(f"    ⚠️  Too little content ({len(clean_text)} chars)")
                return False
            
            # Save files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_dir = self.output_dir / company_id / "initial" / timestamp
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # Save HTML
            html_file = save_dir / f"{page_type}.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Save text
            txt_file = save_dir / f"{page_type}.txt"
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(clean_text)
            
            # Save metadata
            metadata = {
                'company_name': company_name,
                'company_id': company_id,
                'source_url': url,
                'page_type': page_type,
                'crawled_at': datetime.now().isoformat(),
                'content_length': len(clean_text),
                'method': 'playwright'
            }
            
            meta_file = save_dir / f"{page_type}_metadata.json"
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"    ✓ Saved ({len(clean_text):,} chars) [Playwright]")
            time.sleep(self.delay)
            
            return True
            
        except Exception as e:
            print(f"    ✗ Error: {str(e)}")
            return False
    
    def discover_pages(self, base_url):
        """Discover pages - comprehensive patterns"""
        
        pages = {'homepage': base_url}
        
        # Comprehensive patterns
        patterns = {
            'about': ['/about', '/company', '/about-us', '/who-we-are', '/mission'],
            'careers': ['/careers', '/jobs', '/join-us', '/work', '/opportunities'],
            'blog': ['/blog', '/news', '/newsroom', '/insights', '/articles'],
            'products': ['/products', '/product', '/platform', '/solutions', '/features'],
            'pricing': ['/pricing', '/plans', '/price'],
            'contact': ['/contact', '/contact-us'],
            'support': ['/support', '/help'],
            'research': ['/research', '/papers'],
            'team': ['/team', '/people', '/leadership'],
            'api': ['/api', '/developers', '/docs'],
            'security': ['/security', '/trust'],
            'resources': ['/resources', '/learn', '/guides']
        }
        
        print(f"  Testing URL patterns...")
        
        # Use Playwright to test URLs (better than HEAD requests)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            test_page = context.new_page()
            
            for page_type, urls in patterns.items():
                found = False
                for url_path in urls:
                    if found:
                        break
                        
                    test_url = base_url.rstrip('/') + url_path
                    
                    try:
                        response = test_page.goto(test_url, wait_until='domcontentloaded', timeout=10000)
                        
                        # Check if page exists and has content
                        if response and response.status < 400:
                            # Quick check if page has actual content
                            content = test_page.content()
                            soup = BeautifulSoup(content, 'html.parser')
                            text = soup.get_text()
                            
                            # Must have substantial content
                            if len(text.strip()) > 200:
                                pages[page_type] = test_url
                                print(f"    ✓ {page_type}")
                                found = True
                    except:
                        continue
            
            browser.close()
        
        return pages
    
    def scrape_blog_posts(self, blog_url, company_name, company_id, max_posts=5):
        """Extract and scrape individual blog posts"""
        
        print(f"\n  Extracting blog posts...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                
                page.goto(blog_url, wait_until='networkidle')
                page.wait_for_timeout(2000)
                
                # Get page content
                html = page.content()
                browser.close()
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find blog post links
            post_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text().strip()
                
                # Blog post indicators
                if any(x in href.lower() for x in ['/blog/', '/post/', '/article/', '/news/']):
                    full_url = urljoin(blog_url, href)
                    if full_url != blog_url and full_url not in post_links:
                        post_links.append(full_url)
            
            # Scrape posts
            post_count = 0
            for i, post_url in enumerate(post_links[:max_posts], 1):
                if self.scrape_page_with_playwright(post_url, company_name, company_id, f"blog_post_{i}"):
                    post_count += 1
            
            print(f"  ✓ Extracted {post_count} blog posts")
            return post_count
            
        except Exception as e:
            print(f"  ✗ Blog extraction error: {str(e)}")
            return 0
    
    def scrape_company(self, company):
        """Scrape company with Playwright"""
        
        company_name = company.get('company_name')
        company_id = company.get('company_id')
        website = company.get('website')
        
        print(f"\n{'='*60}")
        print(f"SCRAPING: {company_name} [PLAYWRIGHT]")
        print(f"{'='*60}")
        
        if not website:
            return {'company_name': company_name, 'total_pages': 0}
        
        # Reset duplicate tracking
        self.content_hashes[company_id] = set()
        
        # Discover pages
        pages = self.discover_pages(website)
        print(f"\n  Discovered {len(pages)} pages")
        
        # Scrape each page
        success_count = 0
        blog_url = None
        
        for page_type, url in pages.items():
            if self.scrape_page_with_playwright(url, company_name, company_id, page_type):
                success_count += 1
                if page_type == 'blog':
                    blog_url = url
        
        # Extract blog posts
        blog_posts = 0
        if blog_url:
            blog_posts = self.scrape_blog_posts(blog_url, company_name, company_id)
        
        total = success_count + blog_posts
        
        print(f"\n  ✓ Total: {success_count} pages + {blog_posts} blog posts = {total}")
        
        return {
            'company_name': company_name,
            'company_id': company_id,
            'main_pages': success_count,
            'blog_posts': blog_posts,
            'total_pages': total
        }


def scrape_missing_with_playwright():
    """Scrape the 5 missing companies using Playwright"""
    
    # Missing companies
    missing_companies = [
        'OpenAI', 'xAI', 'Midjourney', 'Perplexity AI', 'World Labs'
    ]
    
    print("\n" + "="*70)
    print("SCRAPING MISSING COMPANIES WITH PLAYWRIGHT")
    print("="*70 + "\n")
    
    with open('data/forbes_ai50_seed.json', 'r') as f:
        all_companies = json.load(f)
    
    scraper = PlaywrightScraper(output_dir="data/raw", delay=2.0)
    
    results = []
    
    for name in missing_companies:
        company = next((c for c in all_companies if c['company_name'] == name), None)
        if company:
            result = scraper.scrape_company(company)
            results.append(result)
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    for r in results:
        status = "✅" if r['total_pages'] > 0 else "❌"
        print(f"{status} {r['company_name']:20s} {r['total_pages']} pages")
    
    successful = sum(1 for r in results if r['total_pages'] > 0)
    print(f"\nSuccessful: {successful}/{len(results)}")
    print()


def scrape_javascript_sites():
    """Scrape the 4 JavaScript sites with Playwright"""
    
    js_companies = ['Abridge', 'Captions', 'Photoroom', 'Suno']
    
    print("\n" + "="*70)
    print("SCRAPING JAVASCRIPT SITES WITH PLAYWRIGHT")
    print("="*70 + "\n")
    
    with open('data/forbes_ai50_seed.json', 'r') as f:
        all_companies = json.load(f)
    
    scraper = PlaywrightScraper(output_dir="data/raw", delay=2.0)
    
    for name in js_companies:
        company = next((c for c in all_companies if c['company_name'] == name), None)
        if company:
            scraper.scrape_company(company)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--missing":
            scrape_missing_with_playwright()
        elif sys.argv[1] == "--javascript":
            scrape_javascript_sites()
        else:
            # Scrape specific company
            with open('data/forbes_ai50_seed.json', 'r') as f:
                companies = json.load(f)
            
            company = next((c for c in companies if c['company_name'] == sys.argv[1]), None)
            
            if company:
                scraper = PlaywrightScraper(output_dir="data/raw", delay=1.0)
                scraper.scrape_company(company)
            else:
                print(f"Company not found: {sys.argv[1]}")
    else:
        print("\nUsage:")
        print("  python playwright_scraper.py Suno              # Test one company")
        print("  python playwright_scraper.py --javascript      # Fix 4 JS sites")
        print("  python playwright_scraper.py --missing         # Try 5 missing")
        print()
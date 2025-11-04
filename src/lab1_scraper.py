"""
Lab 1 Complete All-in-One Scraper
Combines best features from all scrapers:
- Fast requests for simple sites
- Selenium for JavaScript sites  
- Playwright for difficult sites
- Blog post extraction
- Duplicate detection
- Comprehensive URL patterns
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse
import hashlib

# Try to import Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except:
    SELENIUM_AVAILABLE = False

# Try to import Playwright
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except:
    PLAYWRIGHT_AVAILABLE = False


class Lab1Scraper:
    """Complete all-in-one scraper with intelligent method selection"""
    
    def __init__(self, output_dir="data/raw", delay=1.5):
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.content_hashes = {}
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        print(f"\n🔧 Available methods:")
        print(f"  Requests:   ✅ Always available (fast)")
        print(f"  Selenium:   {'✅ Available' if SELENIUM_AVAILABLE else '❌ Not installed'}")
        print(f"  Playwright: {'✅ Available' if PLAYWRIGHT_AVAILABLE else '❌ Not installed'}")
    
    def _hash(self, text):
        return hashlib.md5(text[:300].encode()).hexdigest()
    
    def _is_dup(self, text, cid):
        h = self._hash(text)
        if cid not in self.content_hashes:
            self.content_hashes[cid] = set()
        if h in self.content_hashes[cid]:
            return True
        self.content_hashes[cid].add(h)
        return False
    
    def _save_page(self, html, url, cname, cid, ptype, method):
        """Extract text and save files"""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'form', 'button']):
            tag.decompose()
        
        text = soup.get_text(separator='\n')
        lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 2]
        clean = '\n'.join(lines)
        
        if self._is_dup(clean, cid):
            return False
        if len(clean) < 80:
            return False
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        sdir = self.output_dir / cid / "initial" / ts
        sdir.mkdir(parents=True, exist_ok=True)
        
        with open(sdir / f"{ptype}.html", 'w', encoding='utf-8') as f:
            f.write(html)
        
        with open(sdir / f"{ptype}.txt", 'w', encoding='utf-8') as f:
            f.write(clean)
        
        meta = {
            'company_name': cname, 'company_id': cid, 'source_url': url,
            'page_type': ptype, 'crawled_at': datetime.now().isoformat(),
            'content_length': len(clean), 'method': method
        }
        
        with open(sdir / f"{ptype}_metadata.json", 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2)
        
        print(f"    ✓ {ptype} ({len(clean):,} chars) [{method}]")
        time.sleep(self.delay)
        return True
    
    def scrape_with_requests(self, url, cname, cid, ptype):
        """Method 1: Fast requests"""
        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            html = r.text
            
            if 'enable JavaScript' in html[:1000]:
                return None  # Needs JS
            
            return self._save_page(html, url, cname, cid, ptype, 'requests')
        except:
            return None
    
    def scrape_with_selenium(self, url, cname, cid, ptype):
        """Method 2: Selenium for JS sites"""
        if not SELENIUM_AVAILABLE:
            return None
        
        try:
            options = ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            html = driver.page_source
            driver.quit()
            
            return self._save_page(html, url, cname, cid, ptype, 'selenium')
        except:
            return None
    
    def scrape_with_playwright(self, url, cname, cid, ptype):
        """Method 3: Playwright for difficult sites"""
        if not PLAYWRIGHT_AVAILABLE:
            return None
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                except:
                    page.goto(url, wait_until='load', timeout=60000)
                
                page.wait_for_timeout(2000)
                html = page.content()
                browser.close()
            
            return self._save_page(html, url, cname, cid, ptype, 'playwright')
        except:
            return None
    
    def scrape_page(self, url, cname, cid, ptype):
        """
        Intelligent page scraping with cascading methods
        Try: Requests → Selenium → Playwright
        """
        
        print(f"  {ptype}: {url[:50]}...")
        
        # Method 1: Try requests (fastest)
        result = self.scrape_with_requests(url, cname, cid, ptype)
        if result:
            return True
        
        # Method 2: Try Selenium (good for JS)
        result = self.scrape_with_selenium(url, cname, cid, ptype)
        if result:
            return True
        
        # Method 3: Try Playwright (best for difficult sites)
        result = self.scrape_with_playwright(url, cname, cid, ptype)
        if result:
            return True
        
        print(f"    ✗ All methods failed")
        return False
    
    def discover_pages(self, base_url):
        """Discover pages using comprehensive URL patterns"""
        
        pages = {'homepage': base_url}
        
        patterns = {
            'about': ['/about', '/company', '/about-us'],
            'careers': ['/careers', '/jobs', '/work'],
            'blog': ['/blog', '/news', '/newsroom'],
            'products': ['/products', '/product', '/platform', '/solutions'],
            'pricing': ['/pricing', '/plans'],
            'contact': ['/contact'],
            'support': ['/support', '/help'],
            'research': ['/research', '/papers'],
            'team': ['/team', '/people'],
            'api': ['/api', '/developers'],
            'security': ['/security', '/trust'],
            'resources': ['/resources', '/guides']
        }
        
        for ptype, urls in patterns.items():
            for pattern in urls:
                url = base_url.rstrip('/') + pattern
                try:
                    r = self.session.head(url, timeout=5, allow_redirects=True)
                    if r.status_code < 400:
                        pages[ptype] = url
                        break
                except:
                    continue
        
        return pages
    
    def extract_blog_posts(self, blog_url, cname, cid, max_posts=3):
        """Extract individual blog posts"""
        try:
            r = self.session.get(blog_url, timeout=20)
            soup = BeautifulSoup(r.content, 'html.parser')
            
            posts = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if any(x in href.lower() for x in ['/blog/', '/post/', '/article/']):
                    full = urljoin(blog_url, href)
                    if full != blog_url and full not in posts:
                        posts.append(full)
            
            count = 0
            for i, url in enumerate(posts[:max_posts], 1):
                if self.scrape_page(url, cname, cid, f"blog_post_{i}"):
                    count += 1
            
            if count > 0:
                print(f"  ✓ {count} blog posts")
            return count
        except:
            return 0
    
    def scrape_company(self, company):
        """Scrape one company with all available methods"""
        
        cname = company['company_name']
        cid = company['company_id']
        website = company['website']
        
        print(f"\n{'='*60}")
        print(f"{cname}")
        print(f"{'='*60}")
        
        if not website:
            return {'company_name': cname, 'total_pages': 0}
        
        self.content_hashes[cid] = set()
        
        # Discover pages
        pages = self.discover_pages(website)
        print(f"  Discovered {len(pages)} pages")
        
        # Scrape all pages
        success = 0
        blog_url = None
        
        for ptype, url in pages.items():
            if self.scrape_page(url, cname, cid, ptype):
                success += 1
                if ptype == 'blog':
                    blog_url = url
        
        # Extract blog posts
        blog_posts = 0
        if blog_url:
            print(f"\n  Extracting blog posts...")
            blog_posts = self.extract_blog_posts(blog_url, cname, cid)
        
        total = success + blog_posts
        print(f"\n  ✅ Total: {success} pages + {blog_posts} posts = {total}")
        
        return {
            'company_name': cname,
            'company_id': cid,
            'total_pages': total
        }


def main():
    """Scrape all 50 companies"""
    
    with open('data/forbes_ai50_seed.json', 'r') as f:
        companies = json.load(f)
    
    print(f"\n{'='*70}")
    print(f"LAB 1 SCRAPER - ALL 50 COMPANIES")
    print(f"{'='*70}")
    
    scraper = Lab1Scraper(output_dir="data/raw", delay=1.5)
    
    results = []
    start = time.time()
    
    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/50]", end=' ')
        result = scraper.scrape_company(company)
        results.append(result)
    
    elapsed = (time.time() - start) / 60
    total = sum(r['total_pages'] for r in results)
    
    print(f"\n{'='*70}")
    print(f"COMPLETED")
    print(f"{'='*70}")
    print(f"Companies:   {len(results)}/50")
    print(f"Total pages: {total}")
    print(f"Average:     {total/len(results):.1f} pages/company")
    print(f"Time:        {elapsed:.1f} minutes")
    print(f"{'='*70}\n")
    
    with open('data/lab1_scrape_report.json', 'w') as f:
        json.dump({
            'completed_at': datetime.now().isoformat(),
            'total_companies': len(results),
            'total_pages': total,
            'runtime_minutes': round(elapsed, 1),
            'companies': results
        }, f, indent=2)
    
    print("✅ Report: data/lab1_scrape_report.json\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Single company
        with open('data/forbes_ai50_seed.json', 'r') as f:
            companies = json.load(f)
        
        company = next((c for c in companies if c['company_name'] == sys.argv[1]), None)
        
        if company:
            scraper = Lab1Scraper(output_dir="data/raw", delay=1.0)
            scraper.scrape_company(company)
        else:
            print(f"Company not found: {sys.argv[1]}")
    else:
        # Scrape all
        main()

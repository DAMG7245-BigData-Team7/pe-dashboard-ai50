"""
Improved Bulletproof Scraper
- Handles JavaScript sites (Suno)
- Detects duplicate content (Pika)
- Scrapes individual blog posts
- Gets more comprehensive pages
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
import time
from datetime import datetime
from urllib.parse import urljoin
import hashlib

# Selenium for JavaScript sites
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except:
    SELENIUM_AVAILABLE = False
    print("⚠️  Selenium not available - JavaScript sites will be skipped")


class ImprovedBulletproofScraper:
    """Enhanced scraper with JavaScript support and blog post extraction"""
    
    def __init__(self, output_dir="data/raw", delay=2.0):
        self.output_dir = Path(output_dir)
        self.delay = delay
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Track content hashes to detect duplicates
        self.content_hashes = {}
    
    def _get_content_hash(self, text):
        """Get hash of content for duplicate detection"""
        # Use first 500 chars for comparison
        sample = text[:500].strip()
        return hashlib.md5(sample.encode()).hexdigest()
    
    def _is_duplicate_content(self, text, company_id):
        """Check if content is duplicate of already scraped page"""
        content_hash = self._get_content_hash(text)
        
        if company_id not in self.content_hashes:
            self.content_hashes[company_id] = set()
        
        if content_hash in self.content_hashes[company_id]:
            return True
        
        self.content_hashes[company_id].add(content_hash)
        return False
    
    def _needs_javascript(self, html_content):
        """Detect if page requires JavaScript"""
        indicators = [
            'enable JavaScript',
            'JavaScript is required',
            'JavaScript is disabled',
            'requires JavaScript',
            'turn on JavaScript',
            'noscript'
        ]
        
        html_lower = html_content.lower()
        return any(indicator.lower() in html_lower for indicator in indicators)
    
    def _scrape_with_selenium(self, url):
        """Scrape JavaScript-heavy page with Selenium"""
        
        if not SELENIUM_AVAILABLE:
            return None
        
        driver = None
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            
            # Wait for page load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # Extra wait for dynamic content
            
            # Get page source
            html_content = driver.page_source
            
            return html_content
            
        except Exception as e:
            print(f"      Selenium error: {str(e)}")
            return None
        finally:
            if driver:
                driver.quit()
    
    def scrape_page(self, url, company_name, company_id, page_type, use_selenium=False):
        """Scrape one page with JavaScript detection and duplicate checking"""
        
        print(f"  Scraping {page_type}: {url[:60]}...")
        
        try:
            # Try regular scraping first
            if use_selenium:
                html_content = self._scrape_with_selenium(url)
                if not html_content:
                    return False
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                html_content = response.text
            
            # Check if needs JavaScript
            if self._needs_javascript(html_content) and not use_selenium:
                print(f"    ⚠️  Requires JavaScript - retrying with Selenium...")
                return self.scrape_page(url, company_name, company_id, page_type, use_selenium=True)
            
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
            
            # Check for duplicate content
            if self._is_duplicate_content(clean_text, company_id):
                print(f"    ⚠️  Duplicate content - skipping")
                return False
            
            # Check if content is substantial
            if len(clean_text) < 100:
                print(f"    ⚠️  Too little content - skipping")
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
                'method': 'selenium' if use_selenium else 'requests'
            }
            
            meta_file = save_dir / f"{page_type}_metadata.json"
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"    ✓ Saved ({len(clean_text):,} chars)")
            time.sleep(self.delay)
            
            return True
            
        except Exception as e:
            print(f"    ✗ Error: {str(e)}")
            return False
    
    def discover_pages(self, base_url):
        """Discover pages with more comprehensive patterns"""
        
        pages = {'homepage': base_url}
        
        # Expanded patterns for better coverage
        patterns = {
            'about': ['/about', '/company', '/about-us', '/who-we-are', '/mission'],
            'careers': ['/careers', '/jobs', '/join-us', '/work-with-us', '/opportunities'],
            'blog': ['/blog', '/news', '/newsroom', '/insights', '/articles'],
            'products': ['/products', '/product', '/platform', '/solutions', '/services', '/features'],
            'pricing': ['/pricing', '/plans', '/price', '/get-started'],
            'contact': ['/contact', '/contact-us', '/get-in-touch'],
            'support': ['/support', '/help', '/help-center', '/faq'],
            'research': ['/research', '/papers', '/publications'],
            'team': ['/team', '/people', '/leadership', '/our-team'],
            'customers': ['/customers', '/case-studies', '/success-stories'],
            'resources': ['/resources', '/guides', '/documentation', '/docs'],
            'api': ['/api', '/developers', '/developer', '/api-docs'],
            'security': ['/security', '/trust', '/trust-center', '/safety'],
            'partners': ['/partners', '/partnerships'],
            'events': ['/events', '/webinars']
        }
        
        print(f"  Testing URL patterns...")
        
        for page_type, urls in patterns.items():
            for url_path in urls:
                test_url = base_url.rstrip('/') + url_path
                
                try:
                    r = self.session.head(test_url, timeout=5, allow_redirects=True)
                    if r.status_code < 400:
                        pages[page_type] = test_url
                        print(f"    ✓ {page_type}")
                        break
                except:
                    continue
        
        return pages
    
    def scrape_blog_posts(self, blog_url, company_name, company_id, max_posts=5):
        """Scrape individual blog posts from blog page"""
        
        print(f"\n  Extracting individual blog posts from: {blog_url}")
        
        try:
            response = self.session.get(blog_url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find blog post links
            post_links = []
            
            # Common blog post link patterns
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text().strip().lower()
                
                # Check if it looks like a blog post link
                if any(indicator in href.lower() for indicator in ['/blog/', '/post/', '/article/', '/news/']):
                    if any(indicator in text for indicator in ['read more', 'read', 'continue', 'learn more']) or len(text) > 20:
                        full_url = urljoin(blog_url, href)
                        if full_url not in post_links and full_url != blog_url:
                            post_links.append(full_url)
            
            # Scrape individual posts
            post_count = 0
            for i, post_url in enumerate(post_links[:max_posts], 1):
                page_type = f"blog_post_{i}"
                if self.scrape_page(post_url, company_name, company_id, page_type):
                    post_count += 1
            
            print(f"  ✓ Scraped {post_count} blog posts")
            return post_count
            
        except Exception as e:
            print(f"  ✗ Blog post extraction failed: {str(e)}")
            return 0
    
    def scrape_company(self, company):
        """Scrape all pages for one company"""
        
        company_name = company.get('company_name')
        company_id = company.get('company_id')
        website = company.get('website')
        
        print(f"\n{'='*60}")
        print(f"SCRAPING: {company_name}")
        print(f"{'='*60}")
        
        if not website:
            print("  No website URL")
            return {'company_name': company_name, 'pages_scraped': 0}
        
        # Reset duplicate detection for this company
        self.content_hashes[company_id] = set()
        
        # Discover pages
        pages = self.discover_pages(website)
        print(f"\n  Discovered {len(pages)} pages")
        
        # Scrape each page
        success_count = 0
        blog_url = None
        
        for page_type, url in pages.items():
            if self.scrape_page(url, company_name, company_id, page_type):
                success_count += 1
                
                # Track blog URL for later
                if page_type == 'blog':
                    blog_url = url
        
        # Scrape individual blog posts if we found a blog
        blog_posts = 0
        if blog_url and success_count > 0:
            blog_posts = self.scrape_blog_posts(blog_url, company_name, company_id, max_posts=5)
        
        total_pages = success_count + blog_posts
        
        print(f"\n  ✓ Completed: {success_count} main pages + {blog_posts} blog posts = {total_pages} total")
        
        return {
            'company_name': company_name,
            'company_id': company_id,
            'main_pages': success_count,
            'blog_posts': blog_posts,
            'total_pages': total_pages
        }


def scrape_all_companies():
    """Scrape all 50 companies with improved scraper"""
    
    # Load companies
    with open('data/forbes_ai50_seed.json', 'r') as f:
        companies = json.load(f)
    
    print(f"\n{'='*60}")
    print(f"SCRAPING ALL {len(companies)} COMPANIES (IMPROVED)")
    print(f"{'='*60}")
    print("\nFeatures:")
    print("  ✓ JavaScript site handling (Suno, etc.)")
    print("  ✓ Duplicate content detection (Pika, etc.)")
    print("  ✓ Individual blog post extraction")
    print("  ✓ Comprehensive page discovery")
    print()
    
    scraper = ImprovedBulletproofScraper(output_dir="data/raw", delay=2.0)
    
    results = []
    
    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}]")
        result = scraper.scrape_company(company)
        results.append(result)
    
    # Summary
    total_main = sum(r['main_pages'] for r in results)
    total_blog = sum(r['blog_posts'] for r in results)
    total_pages = sum(r['total_pages'] for r in results)
    
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Companies scraped:  {len(results)}")
    print(f"Main pages:         {total_main}")
    print(f"Blog posts:         {total_blog}")
    print(f"Total pages:        {total_pages}")
    print(f"Avg pages/company:  {total_pages/len(results):.1f}")
    print(f"{'='*60}\n")
    
    # Save report
    report = {
        'completed_at': datetime.now().isoformat(),
        'total_companies': len(results),
        'total_main_pages': total_main,
        'total_blog_posts': total_blog,
        'total_pages': total_pages,
        'companies': results
    }
    
    with open('data/improved_scrape_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("Report saved: data/improved_scrape_report.json\n")


def test_javascript_site(company_name="Suno"):
    """Test JavaScript handling"""
    
    with open('data/forbes_ai50_seed.json', 'r') as f:
        companies = json.load(f)
    
    company = next((c for c in companies if c['company_name'] == company_name), None)
    
    if not company:
        print(f"Company not found: {company_name}")
        return
    
    print(f"\n{'='*60}")
    print(f"TESTING JAVASCRIPT HANDLING: {company_name}")
    print(f"{'='*60}\n")
    
    scraper = ImprovedBulletproofScraper(output_dir="data/raw", delay=1.0)
    result = scraper.scrape_company(company)
    
    # Check if it worked
    company_id = company['company_id']
    txt_files = list(Path(f"data/raw/{company_id}").rglob("*.txt"))
    
    if txt_files:
        print(f"\n{'='*60}")
        print("VERIFICATION")
        print(f"{'='*60}")
        
        for txt_file in txt_files[:3]:
            with open(txt_file, 'r') as f:
                content = f.read()
            
            print(f"\n{txt_file.name}:")
            print(f"  Size: {len(content):,} chars")
            print(f"  Preview: {content[:200]}...")
            
            if 'enable JavaScript' in content:
                print(f"  ❌ Still shows JavaScript warning")
            else:
                print(f"  ✅ Has actual content!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            scrape_all_companies()
        elif sys.argv[1] == "--test-js":
            company = sys.argv[2] if len(sys.argv) > 2 else "Suno"
            test_javascript_site(company)
        else:
            # Test one company
            scraper = ImprovedBulletproofScraper(output_dir="data/raw", delay=1.0)
            
            with open('data/forbes_ai50_seed.json', 'r') as f:
                companies = json.load(f)
            
            company = next((c for c in companies if c['company_name'] == sys.argv[1]), None)
            
            if company:
                scraper.scrape_company(company)
            else:
                print(f"Company not found: {sys.argv[1]}")
    else:
        print("\nUsage:")
        print("  python improved_bulletproof_scraper.py Lambda        # Test one")
        print("  python improved_bulletproof_scraper.py --test-js     # Test JavaScript (Suno)")
        print("  python improved_bulletproof_scraper.py --all         # Scrape all 50")
        print()
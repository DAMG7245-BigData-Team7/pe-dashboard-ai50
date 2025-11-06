import re
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from bs4 import BeautifulSoup

from src.utils import (
    logger,
    load_json,
    save_json,
    normalize_company_name,
    ScraperConfig
)


class GrowthMomentumExtractor:
    """Extract growth momentum indicators from scraped data"""
    
    def __init__(self, company_id: str, run_folder: Path):
        self.company_id = company_id
        self.run_folder = run_folder
        
    def extract_all(self) -> Dict:
        """Extract all growth momentum metrics"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Extracting growth momentum for: {self.company_id}")
        logger.info(f"{'='*60}\n")
        
        metrics = {
            'company_id': self.company_id,
            'extracted_at': datetime.utcnow().isoformat() + 'Z',
            'open_roles': self.extract_open_roles(),
            'recent_posts': self.extract_recent_posts(),
            'office_locations': self.extract_office_locations(),
            'partnerships': self.extract_partnerships(),
            'product_mentions': self.extract_product_mentions(),
            'team_size_indicators': self.extract_team_size_indicators()
        }
        
        # Save results
        output_file = self.run_folder / "growth_momentum.json"
        save_json(metrics, output_file)
        
        logger.info(f"Growth momentum extracted:")
        logger.info(f"  - Open roles: {metrics['open_roles']['count'] or 'Not disclosed'}")
        logger.info(f"  - Recent posts: {len(metrics['recent_posts'])}")
        logger.info(f"  - Office locations: {len(metrics['office_locations'])}")
        logger.info(f"  - Partnerships: {len(metrics['partnerships'])}")
        logger.info(f"  - Team indicators: {len(metrics['team_size_indicators'])}")
        
        return metrics
    
    def extract_open_roles(self) -> Dict:
        """Extract job openings from careers page"""
        # CHANGED: New file names without _raw and _clean suffixes
        careers_html = self.run_folder / "careers.html"
        careers_text = self.run_folder / "careers.txt"
        
        result = {
            'count': None,
            'roles': [],
            'extraction_method': None,
            'note': None
        }
        
        # Method 1: Try parsing HTML
        if careers_html.exists():
            try:
                with open(careers_html, 'r', encoding='utf-8') as f:
                    html = f.read()
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Check for common ATS systems
                # Greenhouse
                if 'greenhouse' in html.lower():
                    jobs = soup.find_all('div', {'class': 'opening'})
                    if jobs:
                        result['count'] = len(jobs)
                        result['extraction_method'] = 'greenhouse'
                        result['roles'] = [{'title': j.get_text(strip=True)[:200]} for j in jobs[:20]]
                        return result
                
                # Lever
                if 'lever' in html.lower():
                    jobs = soup.find_all('div', {'class': 'posting'})
                    if jobs:
                        result['count'] = len(jobs)
                        result['extraction_method'] = 'lever'
                        result['roles'] = [{'title': j.get_text(strip=True)[:200]} for j in jobs[:20]]
                        return result
                
                # Workable
                if 'workable' in html.lower():
                    jobs = soup.find_all('li', {'class': re.compile(r'job', re.I)})
                    if jobs:
                        result['count'] = len(jobs)
                        result['extraction_method'] = 'workable'
                        result['roles'] = [{'title': j.get_text(strip=True)[:200]} for j in jobs[:20]]
                        return result
                
                # Generic job listings
                job_patterns = [
                    soup.find_all('div', {'class': re.compile(r'job-listing', re.I)}),
                    soup.find_all('div', {'class': re.compile(r'position', re.I)}),
                    soup.find_all('li', {'class': re.compile(r'job', re.I)}),
                    soup.find_all('article', {'class': re.compile(r'job', re.I)}),
                ]
                
                for jobs in job_patterns:
                    if jobs and len(jobs) > 0:
                        result['count'] = len(jobs)
                        result['extraction_method'] = 'generic_html'
                        result['roles'] = [{'title': j.get_text(strip=True)[:200]} for j in jobs[:20]]
                        return result
                
            except Exception as e:
                logger.error(f"Error parsing careers HTML: {str(e)}")
        
        # Method 2: Try text-based extraction
        if careers_text.exists():
            try:
                with open(careers_text, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                # Look for "X open positions" or "X jobs"
                patterns = [
                    r'(\d+)\s+open\s+(?:position|role|job)',
                    r'(\d+)\s+(?:position|role|job)s?\s+(?:open|available)',
                    r'(\d+)\s+current\s+(?:opening|position)',
                    r'We\'re\s+hiring\s+(\d+)',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1))
                        result['count'] = count
                        result['extraction_method'] = 'text_pattern'
                        result['note'] = f'Found "{match.group(0)}" in text'
                        return result
                
                # Look for job titles (lines with common job keywords)
                job_keywords = [
                    'engineer', 'developer', 'manager', 'designer', 'analyst',
                    'scientist', 'architect', 'director', 'lead', 'senior',
                    'principal', 'staff', 'head of'
                ]
                
                lines = text.split('\n')
                potential_jobs = []
                for line in lines:
                    line = line.strip()
                    if 20 < len(line) < 100:  # Reasonable title length
                        if any(keyword in line.lower() for keyword in job_keywords):
                            potential_jobs.append({'title': line})
                
                if potential_jobs:
                    result['count'] = len(potential_jobs)
                    result['roles'] = potential_jobs[:20]
                    result['extraction_method'] = 'text_keyword_matching'
                    return result
                
            except Exception as e:
                logger.error(f"Error parsing careers text: {str(e)}")
        
        # Method 3: Check if page says "No openings"
        if careers_text.exists():
            try:
                with open(careers_text, 'r', encoding='utf-8') as f:
                    text = f.read().lower()
                
                no_jobs_phrases = [
                    'no current opening',
                    'no open position',
                    'not hiring',
                    'no available position',
                    'check back later'
                ]
                
                if any(phrase in text for phrase in no_jobs_phrases):
                    result['count'] = 0
                    result['extraction_method'] = 'text_negative_match'
                    result['note'] = 'Careers page indicates no current openings'
                    return result
                
            except Exception as e:
                logger.error(f"Error checking for no-jobs text: {str(e)}")
        
        # If all methods fail
        result['note'] = 'Could not extract job count - page may use JavaScript to load jobs dynamically'
        return result
    
    def extract_recent_posts(self) -> List[Dict]:
        """Extract recent blog posts"""
        # CHANGED: New file names
        blog_html = self.run_folder / "blog.html"
        blog_text = self.run_folder / "blog.txt"
        
        posts = []
        
        # Try HTML parsing first
        if blog_html.exists():
            try:
                with open(blog_html, 'r', encoding='utf-8') as f:
                    html = f.read()
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Look for articles
                for article in soup.find_all('article')[:10]:
                    title_elem = article.find(['h1', 'h2', 'h3', 'h4', 'a'])
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        
                        # Try to find date
                        date_elem = article.find(['time', 'span'], {'class': re.compile(r'date|time|publish', re.I)})
                        date = date_elem.get_text(strip=True) if date_elem else None
                        
                        # Try to find link
                        link_elem = article.find('a', href=True)
                        link = link_elem['href'] if link_elem else None
                        
                        if title and len(title) > 5:
                            posts.append({
                                'title': title,
                                'date': date,
                                'link': link,
                                'source': 'blog_html'
                            })
                
                # Also check for divs/sections with post/article classes
                post_containers = soup.find_all(['div', 'section', 'li'], 
                                               {'class': re.compile(r'post|article|blog|news', re.I)})
                
                for container in post_containers[:20]:
                    title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'a'])
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        if title and len(title) > 5 and title not in [p['title'] for p in posts]:
                            date_elem = container.find(['time', 'span'], {'class': re.compile(r'date|time', re.I)})
                            posts.append({
                                'title': title,
                                'date': date_elem.get_text(strip=True) if date_elem else None,
                                'link': None,
                                'source': 'blog_html'
                            })
                
            except Exception as e:
                logger.error(f"Error extracting from blog HTML: {str(e)}")
        
        # Fallback: Extract from clean text
        if not posts and blog_text.exists():
            try:
                with open(blog_text, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                # Look for date patterns followed by titles
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    # Check if line contains a date
                    date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4}', line)
                    if date_match:
                        # Next non-empty line might be the title
                        for j in range(i+1, min(i+4, len(lines))):
                            potential_title = lines[j].strip()
                            if 10 < len(potential_title) < 200:
                                posts.append({
                                    'title': potential_title,
                                    'date': date_match.group(0),
                                    'link': None,
                                    'source': 'blog_text'
                                })
                                break
                
            except Exception as e:
                logger.error(f"Error extracting from blog text: {str(e)}")
        
        # Remove duplicates
        unique_posts = []
        seen_titles = set()
        for post in posts:
            if post['title'] not in seen_titles:
                seen_titles.add(post['title'])
                unique_posts.append(post)
        
        return unique_posts[:10]
    
    def extract_office_locations(self) -> List[str]:
        """Extract office locations"""
        # CHANGED: New file names
        about_text = self.run_folder / "about.txt"
        homepage_text = self.run_folder / "homepage.txt"
    
        # CHANGED: Look for different metadata file name
        company_info_file = self.run_folder / "homepage_metadata.json"
        base_locations = []
    
        if company_info_file.exists():
            try:
                company_info = load_json(company_info_file)
                # Lab1 scraper doesn't include hq info in metadata, skip this
            except:
                pass

        locations = set(base_locations)

        files_to_check = [about_text, homepage_text]
    
        for file_path in files_to_check:
            if not file_path.exists():
                continue
        
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
            
                # Pattern: City, State/Country (most reliable)
                pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*(?:[A-Z]{2}|USA|United States|UK|United Kingdom|Canada|Germany|France|India|Singapore|China|Japan|Australia|Netherlands|Sweden|Switzerland))\b'
                matches = re.findall(pattern, text)
                locations.update(match.strip() for match in matches)
            
            except Exception as e:
                logger.error(f"Error extracting locations from {file_path}: {str(e)}")
    
        # Strict filtering
        filtered = []
    
        # Job titles to exclude
        job_title_words = {
            'officer', 'chief', 'director', 'manager', 'president', 
            'executive', 'head', 'lead', 'senior', 'vp', 'ceo', 'cto', 
            'cfo', 'coo', 'commercial', 'financial', 'operating', 'technology'
        }
    
        for loc in locations:
            loc_lower = loc.lower()
        
            # Skip if contains job title words
            if any(word in loc_lower for word in job_title_words):
                continue
        
            # Keep if it has comma (likely "City, State/Country")
            if ',' in loc:
                # Additional check: both parts should be capitalized
                parts = loc.split(',')
                if all(part.strip() and part.strip()[0].isupper() for part in parts):
                    filtered.append(loc)
    
        return sorted(list(set(filtered)))[:10]
    
    def extract_partnerships(self) -> List[Dict]:
        """Extract partnership mentions"""
        partnerships = []
        
        # CHANGED: New file names
        files_to_check = [
            (self.run_folder / "blog.txt", 'blog'),
            (self.run_folder / "homepage.txt", 'homepage'),
            (self.run_folder / "about.txt", 'about')
        ]
        
        partnership_keywords = [
            'partner', 'partnership', 'collaboration', 'collaborate',
            'integration', 'integrate', 'announces', 'teams up',
            'joins forces', 'strategic alliance', 'working with'
        ]
        
        for file_path, source in files_to_check:
            if not file_path.exists():
                continue
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                lines = text.split('\n')
                for line in lines:
                    line_clean = line.strip()
                    if 30 < len(line_clean) < 300:  # Reasonable length
                        line_lower = line_clean.lower()
                        if any(keyword in line_lower for keyword in partnership_keywords):
                            # Check if it mentions a company name (has capitals and proper nouns)
                            if sum(1 for c in line_clean if c.isupper()) >= 3:
                                partnerships.append({
                                    'text': line_clean,
                                    'source': source
                                })
                
            except Exception as e:
                logger.error(f"Error reading {file_path}: {str(e)}")
        
        # Remove duplicates
        unique_partnerships = []
        seen_texts = set()
        for p in partnerships:
            if p['text'] not in seen_texts:
                seen_texts.add(p['text'])
                unique_partnerships.append(p)
        
        return unique_partnerships[:10]
    
    def extract_product_mentions(self) -> List[str]:
        """Extract product/feature mentions"""
        # CHANGED: New file names and check for products.txt
        product_text = self.run_folder / "products.txt"  # Changed from product_clean.txt
        homepage_text = self.run_folder / "homepage.txt"
        
        products = set()
        
        files_to_check = [product_text, homepage_text]
        
        for file_path in files_to_check:
            if not file_path.exists():
                continue
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                # Look for product-related headings
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Product names are usually:
                    # - Short (20-100 chars)
                    # - Start with capital
                    # - Not all caps (unless acronym)
                    if 10 < len(line) < 100 and line and line[0].isupper():
                        # Skip if it's a common phrase
                        if line.lower().startswith(('the ', 'a ', 'an ', 'we ', 'our ')):
                            continue
                        # Skip if it's all caps and too long (likely a banner)
                        if line.isupper() and len(line) > 30:
                            continue
                        products.add(line)
                
            except Exception as e:
                logger.error(f"Error extracting products from {file_path}: {str(e)}")
        
        return sorted(list(products))[:20]
    
    def extract_team_size_indicators(self) -> List[Dict]:
        """Extract any mentions of team size, headcount, or growth"""
        indicators = []
        
        # CHANGED: New file names
        files_to_check = [
            (self.run_folder / "about.txt", 'about'),
            (self.run_folder / "homepage.txt", 'homepage'),
            (self.run_folder / "blog.txt", 'blog')
        ]
        
        # Patterns for team size mentions
        patterns = [
            (r'(\d+)\+?\s+(?:employee|people|team member|staff)', 'employee_count'),
            (r'team\s+of\s+(\d+)', 'team_size'),
            (r'(\d+)%\s+(?:growth|increase)\s+in\s+(?:headcount|team|staff)', 'growth_rate'),
            (r'expanded?\s+(?:to|by)\s+(\d+)\s+people', 'expansion'),
            (r'hired?\s+(\d+)\s+new', 'recent_hires')
        ]
        
        for file_path, source in files_to_check:
            if not file_path.exists():
                continue
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                for pattern, indicator_type in patterns:
                    matches = re.finditer(pattern, text, re.IGNORECASE)
                    for match in matches:
                        indicators.append({
                            'type': indicator_type,
                            'value': match.group(1),
                            'context': match.group(0),
                            'source': source
                        })
                
            except Exception as e:
                logger.error(f"Error extracting team indicators from {file_path}: {str(e)}")
        
        return indicators[:5]


def extract_growth_for_all_companies():
    """Extract growth momentum for all scraped companies"""
    raw_dir = ScraperConfig.RAW_DIR
    
    if not raw_dir.exists():
        logger.error(f"Raw data directory not found: {raw_dir}")
        return
    
    results = []
    
    # Find all company folders
    for company_folder in raw_dir.iterdir():
        if not company_folder.is_dir():
            continue
        
        company_id = company_folder.name
        
        # CHANGED: Look for initial/ folder instead of full_* folders
        initial_folder = company_folder / "initial"
        if not initial_folder.exists():
            logger.warning(f"No initial folder found for {company_id}")
            continue
        
        # Find the most recent timestamp folder
        run_folders = sorted(initial_folder.iterdir(), reverse=True)
        if not run_folders:
            logger.warning(f"No scrape data found for {company_id}")
            continue
        
        latest_run = run_folders[0]
        
        # Extract growth momentum
        extractor = GrowthMomentumExtractor(company_id, latest_run)
        metrics = extractor.extract_all()
        results.append(metrics)
    
    # Save combined results
    output_file = ScraperConfig.DATA_DIR / "all_growth_momentum.json"
    save_json({
        'extracted_at': datetime.utcnow().isoformat() + 'Z',
        'total_companies': len(results),
        'companies': results
    }, output_file)
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"Growth momentum extraction complete!")
    logger.info(f"Total companies processed: {len(results)}")
    logger.info(f"Results saved to: {output_file}")
    logger.info(f"{'#'*60}\n")


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract growth momentum metrics")
    parser.add_argument('--company-id', type=str, default=None,
                       help='Extract for specific company')
    
    args = parser.parse_args()
    
    if args.company_id:
        # Extract for specific company
        company_folder = ScraperConfig.RAW_DIR / args.company_id
        initial_folder = company_folder / "initial"
        
        if not initial_folder.exists():
            logger.error(f"No initial folder found for {args.company_id}")
        else:
            # Get most recent timestamp folder
            run_folders = sorted(initial_folder.iterdir(), reverse=True)
            if not run_folders:
                logger.error(f"No data found for {args.company_id}")
            else:
                extractor = GrowthMomentumExtractor(args.company_id, run_folders[0])
                extractor.extract_all()
    else:
        # Extract for all companies
        extract_growth_for_all_companies()
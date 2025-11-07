"""
News & Intelligence Scraper for ORBIT
Single file for: Articles + GitHub + LinkedIn + Crunchbase
Does NOT scrape company websites (use your existing lab_scraper for that)

Saves to: data/raw/company_id/news/articles/
"""

import os
import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from urllib.parse import quote, urlparse
import re

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Article:
    """Article with full content"""
    source: str
    title: str
    url: str
    date: str
    author: str
    snippet: str
    full_content: str
    word_count: int
    scraped_at: str
    extraction_success: bool


class ArticleExtractor:
    """Article content extraction with site-specific strategies"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9'
        }
    
    def extract_forbes(self, soup: BeautifulSoup) -> str:
        content_parts = []
        strategies = [
            lambda: soup.find('div', class_=re.compile(r'article-body|article_body')),
            lambda: soup.find('article'),
            lambda: soup.find('div', class_=re.compile(r'article-content-body')),
        ]
        
        for strategy in strategies:
            try:
                container = strategy()
                if container:
                    for p in container.find_all('p'):
                        text = p.get_text(strip=True)
                        if len(text) > 50:
                            content_parts.append(text)
                    if content_parts:
                        break
            except:
                continue
        
        return '\n\n'.join(content_parts)
    
    def extract_techcrunch(self, soup: BeautifulSoup) -> str:
        content_parts = []
        article_body = soup.find('div', class_=re.compile(r'article-content|entry-content'))
        
        if article_body:
            for p in article_body.find_all('p'):
                text = p.get_text(strip=True)
                if len(text) > 40:
                    content_parts.append(text)
        
        return '\n\n'.join(content_parts)
    
    def extract_generic(self, soup: BeautifulSoup) -> str:
        content_parts = []
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()
        
        # Try article tag first
        article_tag = soup.find('article')
        if article_tag:
            for p in article_tag.find_all('p'):
                text = p.get_text(strip=True)
                if len(text) > 50:
                    content_parts.append(text)
            if len(content_parts) >= 3:
                return '\n\n'.join(content_parts)
        
        # Try content containers
        for container in soup.find_all(['div', 'section'], 
            class_=re.compile(r'(article|content|post|entry)', re.I), limit=5):
            temp_parts = []
            for p in container.find_all('p'):
                text = p.get_text(strip=True)
                if len(text) > 50:
                    temp_parts.append(text)
            if len(temp_parts) > len(content_parts):
                content_parts = temp_parts
        
        return '\n\n'.join(content_parts)
    
    def extract_content(self, url: str) -> tuple[str, bool]:
        """Extract content from URL. Returns (content, success)"""
        try:
            time.sleep(1)
            response = requests.get(url, headers=self.headers, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            domain = urlparse(url).netloc.lower()
            
            if 'forbes.com' in domain:
                content = self.extract_forbes(soup)
            elif 'techcrunch.com' in domain:
                content = self.extract_techcrunch(soup)
            else:
                content = self.extract_generic(soup)
            
            content = re.sub(r'\n{3,}', '\n\n', content).strip()
            word_count = len(content.split())
            success = word_count > 100
            
            return content, success
        except Exception as e:
            logger.error(f"  ✗ Extraction error: {str(e)}")
            return "", False


class NewsIntelligenceScraper:
    """
    Scraper for news articles, GitHub, LinkedIn, and Crunchbase data
    Does NOT scrape company websites
    """
    
    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # API Keys
        self.newsapi_key = os.getenv('NEWSAPI_KEY')
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.proxycurl_key = os.getenv('PROXYCURL_API_KEY')
        self.crunchbase_key = os.getenv('CRUNCHBASE_API_KEY')
        
        self.extractor = ArticleExtractor()
    
    # ========== NEWS SOURCES ==========
    
    def search_newsapi(self, company_name: str, max_articles: int = 10) -> List[Article]:
        """Search NewsAPI and extract full content"""
        if not self.newsapi_key:
            return []
        
        articles = []
        try:
            from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            response = requests.get("https://newsapi.org/v2/everything", params={
                'q': f'"{company_name}" AND (AI OR funding OR startup)',
                'from': from_date,
                'sortBy': 'publishedAt',
                'language': 'en',
                'apiKey': self.newsapi_key,
                'pageSize': max_articles
            }, timeout=30)
            
            data = response.json()
            if data['status'] == 'ok':
                logger.info(f"  NewsAPI: Found {len(data['articles'])} articles")
                
                for item in data['articles']:
                    full_content, success = self.extractor.extract_content(item['url'])
                    
                    articles.append(Article(
                        source=item['source']['name'],
                        title=item['title'],
                        url=item['url'],
                        date=item['publishedAt'],
                        author=item.get('author', 'Unknown'),
                        snippet=item.get('description', ''),
                        full_content=full_content,
                        word_count=len(full_content.split()),
                        scraped_at=datetime.now().isoformat(),
                        extraction_success=success
                    ))
                    time.sleep(2)
        except Exception as e:
            logger.error(f"  NewsAPI error: {str(e)}")
        
        return articles
    
    def search_google_news(self, company_name: str, max_articles: int = 10) -> List[Article]:
        """Search Google News RSS and extract full content"""
        articles = []
        try:
            from xml.etree import ElementTree as ET
            
            rss_url = f"https://news.google.com/rss/search?q={quote(company_name)}+AI&hl=en-US&gl=US&ceid=US:en"
            response = requests.get(rss_url, timeout=30)
            
            root = ET.fromstring(response.content)
            items = root.findall('.//item')[:max_articles]
            
            logger.info(f"  Google News: Found {len(items)} articles")
            
            for item in items:
                title_elem = item.find('title')
                link_elem = item.find('link')
                date_elem = item.find('pubDate')
                
                if title_elem is not None and link_elem is not None:
                    url = link_elem.text
                    title = title_elem.text
                    
                    # Extract source from title
                    source = 'Google News'
                    if ' - ' in title:
                        parts = title.rsplit(' - ', 1)
                        title = parts[0]
                        if len(parts) == 2:
                            source = parts[1]
                    
                    full_content, success = self.extractor.extract_content(url)
                    
                    articles.append(Article(
                        source=source,
                        title=title,
                        url=url,
                        date=date_elem.text if date_elem is not None else '',
                        author='Unknown',
                        snippet='',
                        full_content=full_content,
                        word_count=len(full_content.split()),
                        scraped_at=datetime.now().isoformat(),
                        extraction_success=success
                    ))
                    time.sleep(2)
        except Exception as e:
            logger.error(f"  Google News error: {str(e)}")
        
        return articles
    
    def search_techcrunch(self, company_name: str, max_articles: int = 5) -> List[Article]:
        """Search TechCrunch and extract full content"""
        articles = []
        try:
            search_url = f"https://search.techcrunch.com/search;?p={quote(company_name)}"
            response = requests.get(search_url, headers=self.extractor.headers, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links = soup.find_all('a', href=re.compile(r'techcrunch\.com/\d{4}/\d{2}/\d{2}/'))
            seen_urls = set()
            
            logger.info(f"  TechCrunch: Found {len(links)} articles")
            
            for link in links:
                if len(articles) >= max_articles:
                    break
                
                url = link.get('href', '')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    title = link.get_text(strip=True) or 'TechCrunch Article'
                    
                    full_content, success = self.extractor.extract_content(url)
                    
                    if success:
                        articles.append(Article(
                            source='TechCrunch',
                            title=title,
                            url=url,
                            date=datetime.now().isoformat(),
                            author='Unknown',
                            snippet='',
                            full_content=full_content,
                            word_count=len(full_content.split()),
                            scraped_at=datetime.now().isoformat(),
                            extraction_success=success
                        ))
                    time.sleep(2)
        except Exception as e:
            logger.error(f"  TechCrunch error: {str(e)}")
        
        return articles
    
    # ========== API INTEGRATIONS ==========
    
    def get_github_data(self, github_username: str) -> Dict:
        """Get GitHub data"""
        try:
            headers = {'Accept': 'application/vnd.github.v3+json'}
            if self.github_token:
                headers['Authorization'] = f'token {self.github_token}'
            
            # User/org info
            response = requests.get(f"https://api.github.com/users/{github_username}", 
                                  headers=headers, timeout=10)
            
            if response.status_code != 200:
                return {}
            
            user_data = response.json()
            
            # Repositories
            repos_response = requests.get(
                f"https://api.github.com/users/{github_username}/repos?per_page=100&sort=stars",
                headers=headers, timeout=10
            )
            
            repos = repos_response.json() if repos_response.status_code == 200 else []
            
            return {
                'username': github_username,
                'name': user_data.get('name'),
                'followers': user_data.get('followers', 0),
                'public_repos': user_data.get('public_repos', 0),
                'total_stars': sum(r.get('stargazers_count', 0) for r in repos),
                'top_repos': [
                    {
                        'name': r.get('name'),
                        'stars': r.get('stargazers_count', 0),
                        'forks': r.get('forks_count', 0),
                        'language': r.get('language'),
                        'description': r.get('description', ''),
                        'url': r.get('html_url')
                    }
                    for r in sorted(repos, key=lambda x: x.get('stargazers_count', 0), reverse=True)[:5]
                ]
            }
        except Exception as e:
            logger.error(f"  GitHub error: {str(e)}")
            return {}
    
    def get_linkedin_data(self, linkedin_url: str) -> Dict:
        """Get LinkedIn data via Proxycurl"""
        if not self.proxycurl_key:
            return {}
        
        try:
            response = requests.get(
                'https://nubela.co/proxycurl/api/v2/linkedin/company',
                params={'url': linkedin_url},
                headers={'Authorization': f'Bearer {self.proxycurl_key}'},
                timeout=10
            )
            
            data = response.json()
            
            return {
                'company_name': data.get('name'),
                'followers': data.get('follower_count'),
                'employees': data.get('company_size'),
                'industry': data.get('industry'),
                'description': data.get('description'),
                'website': data.get('website'),
                'headquarters': data.get('headquarters'),
                'founded': data.get('founded_year'),
                'specialties': data.get('specialties')
            }
        except Exception as e:
            logger.error(f"  LinkedIn error: {str(e)}")
            return {}
    
    def get_crunchbase_data(self, company_name: str) -> Dict:
        """Get Crunchbase data"""
        if not self.crunchbase_key:
            return {}
        
        try:
            response = requests.get(
                f'https://api.crunchbase.com/api/v4/entities/organizations/{company_name}',
                headers={'X-cb-user-key': self.crunchbase_key},
                timeout=10
            )
            
            data = response.json()
            properties = data.get('properties', {})
            
            return {
                'company_name': properties.get('name'),
                'short_description': properties.get('short_description'),
                'funding_total': properties.get('funding_total', {}).get('value'),
                'funding_total_currency': properties.get('funding_total', {}).get('currency'),
                'num_funding_rounds': properties.get('num_funding_rounds'),
                'last_funding_type': properties.get('last_funding_type'),
                'last_funding_at': properties.get('last_funding_at'),
                'employee_count': properties.get('num_employees_enum'),
                'founded_on': properties.get('founded_on'),
                'categories': [cat.get('value') for cat in properties.get('categories', [])]
            }
        except Exception as e:
            logger.error(f"  Crunchbase error: {str(e)}")
            return {}
    
    # ========== MAIN SCRAPE METHOD ==========
    
    def scrape_company(
        self,
        company_id: str,
        company_name: str,
        github_username: Optional[str] = None,
        linkedin_url: Optional[str] = None,
        max_articles_per_source: int = 5
    ) -> Dict:
        """
        Scrape news, GitHub, LinkedIn, and Crunchbase data
        Saves to: data/raw/company_id/news/articles/
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"SCRAPING: {company_name} (ID: {company_id})")
        logger.info(f"{'='*80}")
        
        # 1. Collect articles
        logger.info("\n📰 Collecting articles...")
        all_articles = []
        
        if self.newsapi_key:
            all_articles.extend(self.search_newsapi(company_name, max_articles_per_source))
        
        all_articles.extend(self.search_google_news(company_name, max_articles_per_source))
        all_articles.extend(self.search_techcrunch(company_name, max_articles_per_source))
        
        # Remove duplicates
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            if article.url not in seen_urls and article.full_content:
                seen_urls.add(article.url)
                unique_articles.append(article)
        
        # 2. Get API data
        logger.info("\n🔗 Fetching API data...")
        
        github_data = self.get_github_data(github_username) if github_username else {}
        linkedin_data = self.get_linkedin_data(linkedin_url) if linkedin_url else {}
        crunchbase_data = self.get_crunchbase_data(company_name)
        
        # Summary
        successful = len([a for a in unique_articles if a.extraction_success])
        total_words = sum(a.word_count for a in unique_articles)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"✅ COMPLETE: {company_name}")
        logger.info(f"{'='*80}")
        logger.info(f"  📰 Articles: {len(unique_articles)} ({successful} full)")
        logger.info(f"  📝 Total words: {total_words:,}")
        logger.info(f"  🐙 GitHub repos: {github_data.get('public_repos', 0)}")
        logger.info(f"  💼 LinkedIn followers: {linkedin_data.get('followers', 0):,}")
        
        result = {
            'company_id': company_id,
            'company_name': company_name,
            'articles': [asdict(a) for a in unique_articles],
            'github': github_data,
            'linkedin': linkedin_data,
            'crunchbase': crunchbase_data,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Save data
        self._save_data(result)
        
        return result
    
    def _save_data(self, result: Dict):
        """Save to data/raw/company_id/news/articles/"""
        company_id = result['company_id']
        
        # Create directories
        news_dir = self.output_dir / company_id / "news"
        articles_dir = news_dir / "articles"
        articles_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Save company_data.json
        company_data = {
            'company_id': result['company_id'],
            'company_name': result['company_name'],
            'articles': result['articles'],
            'github': result['github'],
            'linkedin': result['linkedin'],
            'crunchbase': result['crunchbase'],
            'scraped_at': result['scraped_at']
        }
        
        with open(news_dir / "company_data.json", 'w', encoding='utf-8') as f:
            json.dump(company_data, f, indent=2, ensure_ascii=False)
        
        # 2. Save all_articles.csv
        if result['articles']:
            import pandas as pd
            df = pd.DataFrame(result['articles'])
            df.to_csv(articles_dir / "all_articles.csv", index=False, encoding='utf-8')
        
        # 3. Save individual article files
        for i, article in enumerate(result['articles'], 1):
            # JSON
            with open(articles_dir / f"article_{i:03d}.json", 'w', encoding='utf-8') as f:
                json.dump(article, f, indent=2, ensure_ascii=False)
            
            # TXT
            article_text = f"""{'='*80}
{"✓ FULL EXTRACTION" if article['extraction_success'] else "⚠ PARTIAL"}
{'='*80}
SOURCE: {article['source']}
TITLE: {article['title']}
URL: {article['url']}
DATE: {article['date']}
AUTHOR: {article['author']}
WORD COUNT: {article['word_count']}
{'='*80}

{article['full_content']}
"""
            with open(articles_dir / f"article_{i:03d}.txt", 'w', encoding='utf-8') as f:
                f.write(article_text)
        
        logger.info(f"\n💾 Saved to: {news_dir}")


def main():
    """CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='News & Intelligence Scraper')
    parser.add_argument('--config', help='JSON config file')
    parser.add_argument('--company', help='Company name')
    parser.add_argument('--id', help='Company ID')
    parser.add_argument('--github', help='GitHub username')
    parser.add_argument('--linkedin', help='LinkedIn URL')
    parser.add_argument('--articles', type=int, default=5, help='Max articles per source')
    
    args = parser.parse_args()
    
    scraper = NewsIntelligenceScraper()
    
    if args.config:
        # Batch mode
        with open(args.config, 'r') as f:
            companies = json.load(f)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"BATCH MODE: {len(companies)} companies")
        logger.info(f"{'='*80}\n")
        
        for i, company in enumerate(companies, 1):
            try:
                company_id = company.get('company_id')
                company_name = company.get('company_name')
                linkedin = company.get('linkedin')
                
                # Try to get or infer GitHub username
                github = company.get('github')
                if not github and company_id:
                    # Try common patterns: openai -> openai, scale-ai -> scaleapi, etc.
                    github = company_id.replace('-', '')  # Try without dashes first
                
                logger.info(f"\n[{i}/{len(companies)}] {company_name}")
                
                scraper.scrape_company(
                    company_id=company_id,
                    company_name=company_name,
                    github_username=github,
                    linkedin_url=linkedin,
                    max_articles_per_source=args.articles
                )
                
                # Rate limiting between companies
                if i < len(companies):
                    logger.info(f"\n⏳ Waiting 5s before next company...\n")
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Error scraping {company.get('company_name', 'Unknown')}: {e}")
                import traceback
                traceback.print_exc()
                continue
    
    elif args.company:
        # Single company
        scraper.scrape_company(
            company_id=args.id or args.company.lower().replace(' ', '-'),
            company_name=args.company,
            github_username=args.github,
            linkedin_url=args.linkedin,
            max_articles_per_source=args.articles
        )
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
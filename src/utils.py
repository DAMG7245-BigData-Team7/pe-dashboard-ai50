import os
import json
import hashlib
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from fake_useragent import UserAgent
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScraperConfig:
    """Configuration for scraper behavior"""
    
    # Directories
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    
    # Scraping behavior
    REQUEST_TIMEOUT = 30
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 5  # seconds
    MIN_DELAY = 2  # Random delay between requests
    MAX_DELAY = 5
    
    # User agent rotation
    USER_AGENT = UserAgent()
    
    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Generate random headers for each request"""
        return {
            'User-Agent': cls.USER_AGENT.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    @classmethod
    def random_delay(cls):
        """Add random delay to mimic human behavior"""
        time.sleep(random.uniform(cls.MIN_DELAY, cls.MAX_DELAY))


def ensure_directory(path: Path) -> Path:
    """Create directory if it doesn't exist"""
    path.mkdir(parents=True, exist_ok=True)
    return path


def compute_hash(content: str) -> str:
    """Compute MD5 hash of content for change detection"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def save_json(data: dict, filepath: Path):
    """Save data as JSON with pretty printing"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved JSON to {filepath}")


def load_json(filepath: Path) -> dict:
    """Load JSON file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_company_name(name: str) -> str:
    """Normalize company name for file system"""
    # Remove special characters, convert to lowercase
    normalized = name.lower()
    normalized = normalized.replace(' ', '_')
    normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
    return normalized


def get_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.utcnow().isoformat() + 'Z'


def create_run_folder(company_id: str, run_type: str = "full") -> Path:
    """
    Create a folder for this scraping run
    Structure: data/raw/{company_id}/{run_type}_{timestamp}/
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_folder = ScraperConfig.RAW_DIR / company_id / f"{run_type}_{timestamp}"
    ensure_directory(run_folder)
    return run_folder


def validate_company_data(company: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate company data from JSON
    Returns: (is_valid, error_message)
    """
    # Check required fields
    if 'company_name' not in company:
        return False, "Missing company_name"
    
    if 'website' not in company or not company['website']:
        return False, "Missing website"
    
    # Check for placeholders
    if 'PLACEHOLDER' in company['company_name'].upper():
        return False, "Placeholder company name"
    
    if 'example.com' in company['website']:
        return False, "Example/placeholder website"
    
    # Validate URL format
    website = company['website']
    if not website.startswith('http'):
        return False, f"Invalid URL format: {website}"
    
    return True, None


def load_and_validate_companies(filepath: Path) -> Tuple[List[Dict], List[Dict]]:
    """
    Load companies and separate valid from invalid
    Returns: (valid_companies, invalid_companies)
    """
    companies = load_json(filepath)
    
    valid = []
    invalid = []
    
    for company in companies:
        is_valid, error = validate_company_data(company)
        if is_valid:
            valid.append(company)
        else:
            invalid.append({
                **company,
                'validation_error': error
            })
            logger.warning(f"Invalid company data: {company.get('company_name', 'Unknown')} - {error}")
    
    logger.info(f"Validation complete: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid
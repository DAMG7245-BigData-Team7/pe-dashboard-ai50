# scripts/populate_ai50_seed.py
import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path

def scrape_forbes_ai50():
    """Scrape the Forbes AI 50 list"""
    url = "https://www.forbes.com/lists/ai50/"
    
    # You'll need to handle Forbes' dynamic loading
    # Consider using Selenium or Playwright for JavaScript-heavy pages
    
    companies = []
    # Parse the page and extract:
    # - company_name
    # - website
    # - linkedin (if available)
    # - category (if listed)
    
    return companies

def save_seed_data(companies):
    output_path = Path("data/forbes_ai50_seed.json")
    with open(output_path, 'w') as f:
        json.dump(companies, f, indent=2)
    print(f"Saved {len(companies)} companies to {output_path}")

if __name__ == "__main__":
    companies = scrape_forbes_ai50()
    save_seed_data(companies)
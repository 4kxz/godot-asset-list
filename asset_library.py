import time
import pandas as pd
import logging
from requests import get, RequestException
from bs4 import BeautifulSoup
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='godot_scraper.log'
)

@dataclass
class AssetInfo:
    name: str
    asset_url: str
    repo_url: str
    stars: str
    godot_version: str
    last_updated: str

class GodotAssetScraper:
    BASE_URL = "https://godotengine.org"
    
    def __init__(self, delay: float = 0.1):
        self.delay = delay
        self.asset_dict: Dict[str, AssetInfo] = {}
        self.failed_urls = set()

    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Make HTTP request with error handling and retry logic."""
        try:
            response = get(url)
            response.raise_for_status()
            time.sleep(self.delay)  # Rate limiting
            return BeautifulSoup(response.text, 'html.parser')
        except RequestException as e:
            logging.error(f"Failed to fetch {url}: {str(e)}")
            return None

    def _parse_stars(self, soup: BeautifulSoup, repo_url: str) -> str:
        """Parse star count from repository page."""
        if 'github' in repo_url:
            return soup.select_one('.js-social-count').get('title', "").replace(',', '')
        elif 'gitlab' in repo_url:
            return soup.select_one('.star-count').text.strip()
        return "0"

    def _clean_repo_url(self, repo_url: str) -> str:
        """Clean repository URL to standard format."""
        if 'github' in repo_url:
            return '/'.join(repo_url.split('/', 5)[:5])
        return repo_url

    def scrape_asset(self, item: BeautifulSoup) -> Optional[AssetInfo]:
        """Scrape individual asset information."""
        try:
            header = item.select_one('.asset-header')
            asset_url = self.BASE_URL + header.get('href')
            
            asset_soup = self._make_request(asset_url)
            if not asset_soup:
                return None
                
            repo_url = asset_soup.select_one('.container a.btn-default').get('href')
            repo_url = self._clean_repo_url(repo_url)
            
            repo_soup = self._make_request(repo_url)
            stars = self._parse_stars(repo_soup, repo_url) if repo_soup else "0"
            
            return AssetInfo(
                name=item.select_one('.asset-title h4').text.strip(),
                asset_url=asset_url,
                repo_url=repo_url,
                stars=stars,
                godot_version=item.select_one('.asset-tags .label-info').text.strip(),
                last_updated=item.select_one('.asset-footer span').text.rsplit('|')[-1].strip()
            )
        except Exception as e:
            logging.error(f"Failed to scrape asset: {str(e)}")
            self.failed_urls.add(asset_url)
            return None

    def scrape_all(self, max_pages: int = 1000) -> None:
        """Scrape all assets."""
        for page in range(max_pages):
            logging.info(f"Scraping page {page + 1}/{max_pages}")

            page_url = f'{self.BASE_URL}/asset-library/asset?max_results=100&page={page}&sort=updated'
            soup = self._make_request(page_url)
            if not soup:
                continue
                
            for item in soup.select('.asset-item'):
                if asset_info := self.scrape_asset(item):
                    logging.info(f"Scraped: {asset_info.name}")
                    if previous_info := self.asset_dict.get(asset_info.asset_url):
                        if previous_info.godot_version > asset_info.godot_version:
                            continue
                    self.asset_dict[asset_info.asset_url] = asset_info
        
        logging.info(f"Scraping completed. Total assets: {len(self.asset_dict)}")
        logging.info(f"Failed URLs: {len(self.failed_urls)}")

    def save_results(self, filename: str) -> None:
        """Save results to specified CSV file."""
        df = pd.DataFrame.from_records([vars(asset) for asset in self.asset_dict.values()])
        if not df.empty:
            df["stars"] = pd.to_numeric(df["stars"].apply(
                lambda x: x.replace(".", "").replace("k", "00")
            ))
            df = df.sort_values("stars", ascending=False)
            
        df.to_csv(filename, index=False)
        logging.info(f"Results saved to {filename}")

def main():
    scraper = GodotAssetScraper()
    scraper.scrape_all(34)
    today = datetime.today()
    scraper.save_results(f"godot_assets_{today:%Y-%m-%d}.csv")

if __name__ == "__main__":
    main()

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import random
import re
import os
import json
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import argparse
from more_itertools import chunked

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Configuration
NUM_NIGHTS = 1
NUM_DATES = 3
CHECKPOINT_FILE = "checkpoint.json"
RESULTS_FILE = "cheapest_10k_hotels_by_city.csv"
PROXY_TEST_URL = "https://httpbin.org/ip"
MAX_RETRIES = 3
PROXY_TIMEOUT = 10
PAGE_TIMEOUT = 30000
MAX_CONCURRENT = int(os.environ.get('MAX_CONCURRENT', '5'))  # Configurable concurrency

@dataclass
class Proxy:
    """Proxy configuration"""
    server: str
    username: str
    password: str
    is_working: bool = True
    failures: int = 0

class ProxyManager:
    """Manages proxy rotation and health checking"""
    def __init__(self, proxy_file: str):
        self.proxies = self._load_proxies(proxy_file)
        self.working_proxies = []
        self.current_index = 0
        self.lock = asyncio.Lock()
        
    def _load_proxies(self, proxy_file: str) -> List[Proxy]:
        """Load proxies from file"""
        proxies = []
        with open(proxy_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and line.count(":") == 3:
                    host, port, user, pwd = line.split(":")
                    proxies.append(Proxy(
                        server=f"http://{host}:{port}",
                        username=user,
                        password=pwd
                    ))
        logging.info(f"Loaded {len(proxies)} proxies")
        return proxies
    
    async def test_proxy(self, proxy: Proxy) -> bool:
        """Test if proxy is working"""
        try:
            auth = aiohttp.BasicAuth(proxy.username, proxy.password)
            connector = aiohttp.TCPConnector(ssl=False)
            timeout = aiohttp.ClientTimeout(total=PROXY_TIMEOUT)
            
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                auth=auth
            ) as session:
                async with session.get(
                    PROXY_TEST_URL,
                    proxy=proxy.server
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.debug(f"Proxy {proxy.server} failed test: {e}")
            return False
    
    async def initialize(self, sample_size: int = 20):
        """Test a sample of proxies to find working ones"""
        logging.info("Testing proxies...")
        sample = random.sample(self.proxies, min(sample_size, len(self.proxies)))
        
        tasks = [self.test_proxy(proxy) for proxy in sample]
        results = await asyncio.gather(*tasks)
        
        self.working_proxies = [
            proxy for proxy, is_working in zip(sample, results) if is_working
        ]
        
        logging.info(f"Found {len(self.working_proxies)} working proxies out of {len(sample)} tested")
        
        if not self.working_proxies:
            raise Exception("No working proxies found!")
    
    async def get_proxy(self) -> Optional[Proxy]:
        """Get next working proxy"""
        async with self.lock:
            if not self.working_proxies:
                return None
                
            # Round-robin through working proxies
            proxy = self.working_proxies[self.current_index % len(self.working_proxies)]
            self.current_index += 1
            
            # If proxy has too many failures, remove it
            if proxy.failures > 3:
                self.working_proxies.remove(proxy)
                logging.warning(f"Removed failed proxy: {proxy.server}")
                
            return proxy if self.working_proxies else None
    
    async def mark_proxy_failed(self, proxy: Proxy):
        """Mark proxy as failed"""
        async with self.lock:
            proxy.failures += 1

class CheckpointManager:
    """Manages progress checkpointing"""
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.completed = self._load_checkpoint()
    
    def _load_checkpoint(self) -> set:
        """Load completed cities from checkpoint"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                return set(data.get('completed', []))
        return set()
    
    def save_checkpoint(self):
        """Save current progress"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump({'completed': list(self.completed)}, f)
    
    def is_completed(self, city: str) -> bool:
        """Check if city is already completed"""
        return city in self.completed
    
    def mark_completed(self, city: str):
        """Mark city as completed"""
        self.completed.add(city)
        self.save_checkpoint()

async def scrape_city_date(
    city: str,
    checkin: datetime,
    checkout: datetime,
    proxy_manager: ProxyManager
) -> Optional[Dict]:
    """Scrape hotels for a specific city and date"""
    
    for attempt in range(MAX_RETRIES):
        proxy = await proxy_manager.get_proxy()
        if not proxy:
            logging.error("No working proxies available")
            return None
            
        browser = None
        try:
            async with async_playwright() as p:
                # Launch browser with proxy
                browser = await p.chromium.launch(
                    proxy={
                        "server": proxy.server,
                        "username": proxy.username,
                        "password": proxy.password
                    },
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                
                page = await context.new_page()
                
                # Navigate to site
                await page.goto("https://www.aadvantagehotels.com/", timeout=PAGE_TIMEOUT)
                await asyncio.sleep(random.uniform(2, 4))
                
                # Fill city
                city_input = await page.wait_for_selector(
                    'input[placeholder="Enter a city, airport, or landmark"]',
                    timeout=15000
                )
                await city_input.click()
                await asyncio.sleep(1)
                await city_input.fill(city, force=True)
                await asyncio.sleep(2)
                
                # Select from dropdown
                dropdown_options = await page.query_selector_all('ul[role="listbox"] li')
                if dropdown_options:
                    await dropdown_options[0].click()
                else:
                    await page.keyboard.press('ArrowDown')
                    await page.keyboard.press('Enter')
                await asyncio.sleep(1)
                
                # Fill dates
                checkin_input = await page.wait_for_selector('input[placeholder="Check-in"]', timeout=10000)
                checkout_input = await page.wait_for_selector('input[placeholder="Check-out"]', timeout=10000)
                
                await checkin_input.click()
                await checkin_input.fill(checkin.strftime("%m/%d/%Y"), force=True)
                await asyncio.sleep(1)
                
                await checkout_input.click()
                await checkout_input.fill(checkout.strftime("%m/%d/%Y"), force=True)
                await asyncio.sleep(1)
                
                # Search
                await page.get_by_role("button", name="Search").click()
                
                # Wait for results
                try:
                    await page.wait_for_url("**/search**", timeout=20000)
                except:
                    if "/search" not in page.url:
                        raise Exception("Search failed")
                        
                await asyncio.sleep(3)
                
                # Sort by highest miles
                current_url = page.url
                if "sort=" not in current_url:
                    sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
                else:
                    sorted_url = re.sub(r'sort=[^&]*', 'sort=milesHighest', current_url)
                
                await page.goto(sorted_url, timeout=PAGE_TIMEOUT)
                await asyncio.sleep(3)
                
                # Find hotels
                try:
                    await page.wait_for_selector('[data-testid="hotel-name"]', timeout=10000)
                    hotel_name_elements = await page.query_selector_all('[data-testid="hotel-name"]')
                except:
                    logging.info(f"No hotels found for {city} on {checkin}")
                    return None
                
                # Parse hotels
                cheapest_10k = None
                
                for i, name_elem in enumerate(hotel_name_elements[:20]):
                    try:
                        name = await name_elem.text_content()
                        
                        # Get parent card
                        card = await name_elem.evaluate_handle('el => el.closest("div[data-testid]") || el.parentElement?.parentElement?.parentElement')
                        if not card:
                            continue
                        
                        # Extract price
                        price = None
                        price_elem = await card.query_selector('[data-testid="earn-price"]')
                        if price_elem:
                            price_text = await price_elem.text_content()
                            # Better price extraction
                            price_match = re.search(r'[£€$¥₹]\s*([\d,]+\.?\d*)', price_text)
                            if not price_match:
                                price_match = re.search(r'([\d,]+\.?\d*)', price_text)
                            if price_match:
                                price = float(price_match.group(1).replace(',', ''))
                        
                        # Extract points
                        points = None
                        miles_elem = await card.query_selector('[data-testid="tier-earn-rewards"]')
                        if miles_elem:
                            miles_text = await miles_elem.text_content()
                            miles_numbers = re.findall(r'([\d,]+)', miles_text)
                            if miles_numbers:
                                all_numbers = [int(n.replace(',', '')) for n in miles_numbers]
                                # Filter for reasonable point values
                                valid_points = [n for n in all_numbers if n >= 1000]
                                if valid_points:
                                    points = max(valid_points)
                        
                        if name and price and points and points == 10000:
                            hotel_data = {
                                'City': city,
                                'Hotel': name.strip(),
                                'Price': price,
                                'Points': points,
                                'Check-in': checkin.strftime("%Y-%m-%d"),
                                'Check-out': checkout.strftime("%Y-%m-%d"),
                                'Cost per Point': price / points
                            }
                            
                            if not cheapest_10k or price < cheapest_10k['Price']:
                                cheapest_10k = hotel_data
                                
                    except Exception as e:
                        logging.debug(f"Error parsing hotel {i}: {e}")
                        continue
                
                return cheapest_10k
                
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for {city}: {e}")
            await proxy_manager.mark_proxy_failed(proxy)
            
        finally:
            if browser:
                await browser.close()
                
        # Wait before retry
        await asyncio.sleep(random.uniform(2, 5))
    
    return None

async def scrape_city(
    city: str,
    proxy_manager: ProxyManager,
    checkpoint_manager: CheckpointManager,
    results_manager
) -> Optional[Dict]:
    """Scrape all dates for a city"""
    
    # Skip if already completed
    if checkpoint_manager.is_completed(city):
        logging.info(f"Skipping {city} - already completed")
        return None
    
    logging.info(f"Scraping {city}...")
    city_cheapest = None
    
    for day in range(NUM_DATES):
        checkin = datetime.now() + timedelta(days=day)
        checkout = checkin + timedelta(days=NUM_NIGHTS)
        
        result = await scrape_city_date(city, checkin, checkout, proxy_manager)
        
        if result and (not city_cheapest or result['Price'] < city_cheapest['Price']):
            city_cheapest = result
        
        # Small delay between dates
        await asyncio.sleep(random.uniform(1, 3))
    
    # Save result
    if city_cheapest:
        results_manager.add_result(city_cheapest)
        logging.info(f"✅ Found 10K hotel in {city}: {city_cheapest['Hotel']} - ${city_cheapest['Price']}")
    else:
        logging.info(f"❌ No 10K hotels found in {city}")
    
    # Mark as completed
    checkpoint_manager.mark_completed(city)
    
    return city_cheapest

class ResultsManager:
    """Manages results saving"""
    def __init__(self, results_file: str):
        self.results_file = results_file
        self.results = []
        self.lock = asyncio.Lock()
        self._load_existing()
    
    def _load_existing(self):
        """Load existing results if file exists"""
        if os.path.exists(self.results_file):
            df = pd.read_csv(self.results_file)
            self.results = df.to_dict('records')
            logging.info(f"Loaded {len(self.results)} existing results")
    
    async def add_result(self, result: Dict):
        """Add a result and save to file"""
        async with self.lock:
            self.results.append(result)
            df = pd.DataFrame(self.results)
            df.to_csv(self.results_file, index=False)

async def worker(
    cities_queue: asyncio.Queue,
    proxy_manager: ProxyManager,
    checkpoint_manager: CheckpointManager,
    results_manager: ResultsManager,
    worker_id: int
):
    """Worker coroutine for processing cities"""
    while True:
        try:
            city = await cities_queue.get()
            if city is None:  # Sentinel value
                break
                
            logging.info(f"Worker {worker_id} processing {city}")
            await scrape_city(city, proxy_manager, checkpoint_manager, results_manager)
            
        except Exception as e:
            logging.error(f"Worker {worker_id} error: {e}")
        finally:
            cities_queue.task_done()

async def main(
    cities_file: str = "cities_top200.txt",
    proxy_file: str = "webshare_proxies.txt",
    batch_start: int = 0,
    batch_size: int = None,
    max_concurrent: int = MAX_CONCURRENT
):
    """Main scraping function"""
    
    # Load cities
    with open(cities_file, 'r') as f:
        all_cities = [line.strip() for line in f if line.strip()]
    
    # Apply batch processing if specified
    if batch_size:
        cities = all_cities[batch_start:batch_start + batch_size]
        logging.info(f"Processing batch: cities {batch_start} to {batch_start + len(cities)}")
    else:
        cities = all_cities
    
    # Initialize managers
    proxy_manager = ProxyManager(proxy_file)
    await proxy_manager.initialize()
    
    checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)
    results_manager = ResultsManager(RESULTS_FILE)
    
    # Filter out completed cities
    cities_to_process = [c for c in cities if not checkpoint_manager.is_completed(c)]
    logging.info(f"Cities to process: {len(cities_to_process)} out of {len(cities)}")
    
    if not cities_to_process:
        logging.info("All cities already completed!")
        return
    
    # Create queue and workers
    cities_queue = asyncio.Queue()
    for city in cities_to_process:
        await cities_queue.put(city)
    
    # Add sentinel values
    for _ in range(max_concurrent):
        await cities_queue.put(None)
    
    # Start workers
    workers = [
        asyncio.create_task(
            worker(cities_queue, proxy_manager, checkpoint_manager, results_manager, i)
        )
        for i in range(max_concurrent)
    ]
    
    # Wait for completion
    await asyncio.gather(*workers)
    
    logging.info(f"Scraping complete! Total results: {len(results_manager.results)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AA Hotels Scraper")
    parser.add_argument("--cities", default="cities_top200.txt", help="Cities file")
    parser.add_argument("--proxies", default="webshare_proxies.txt", help="Proxies file")
    parser.add_argument("--batch-start", type=int, default=0, help="Batch start index")
    parser.add_argument("--batch-size", type=int, default=None, help="Batch size")
    parser.add_argument("--concurrent", type=int, default=MAX_CONCURRENT, help="Max concurrent workers")
    
    args = parser.parse_args()
    
    asyncio.run(main(
        cities_file=args.cities,
        proxy_file=args.proxies,
        batch_start=args.batch_start,
        batch_size=args.batch_size,
        max_concurrent=args.concurrent
    ))

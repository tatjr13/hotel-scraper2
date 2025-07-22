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
PROXY_TIMEOUT = 30
PAGE_TIMEOUT = 60000
NUM_NIGHTS = 1
NUM_DATES = 3
CHECKPOINT_FILE = "checkpoint.json"
RESULTS_FILE = "cheapest_10k_hotels_by_city.csv"
PROXY_TEST_URL = "https://httpbin.org/ip"
MAX_RETRIES = 3
MAX_CONCURRENT = int(os.environ.get('MAX_CONCURRENT', '5'))

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
        """Load proxies from file, robustly handling multiple formats."""
        proxies = []
        logging.info(f"Attempting to load proxies from {proxy_file}...")
        
        if not os.path.exists(proxy_file):
            logging.error(f"Proxy file not found: {proxy_file}")
            return proxies
            
        with open(proxy_file, 'r') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                # Clean the line by removing any protocol prefixes
                cleaned_line = line.replace("http://", "").replace("https://", "")

                # Check for the user:pass@host:port format
                if '@' in cleaned_line:
                    try:
                        creds_part, host_part = cleaned_line.split('@', 1)
                        user, pwd = creds_part.split(':', 1)
                        host, port = host_part.split(':', 1)
                        
                        server_url = f"http://{host}:{port}"
                        logging.debug(f'Parsed proxy: host="{host}", port="{port}", server="{server_url}"')
                        
                        proxies.append(Proxy(
                            server=server_url,
                            username=user,
                            password=pwd
                        ))
                        continue
                    except ValueError as e:
                        logging.warning(f"Skipping malformed proxy line #{i+1} (expected user:pass@host:port): {line}")
                        continue
                
                # Fallback to check for host:port:user:pass format
                parts = cleaned_line.split(':')
                if len(parts) == 4:
                    host, port, user, pwd = parts
                    server_url = f"http://{host}:{port}"
                    logging.debug(f'Parsed proxy: host="{host}", port="{port}", server="{server_url}"')
                    
                    proxies.append(Proxy(
                        server=server_url,
                        username=user,
                        password=pwd
                    ))
                else:
                    logging.warning(f"Skipping malformed proxy line #{i+1}. Expected format 'host:port:user:pass' or 'user:pass@host:port'. Line: '{line}'")
        
        logging.info(f"Successfully loaded {len(proxies)} valid proxies.")
        
        if not proxies:
            logging.error("No valid proxies could be loaded from the file. Please check the file format.")

        return proxies
    
    async def test_proxy(self, proxy: Proxy) -> bool:
        """Test if proxy is working and log detailed errors."""
        try:
            auth = aiohttp.BasicAuth(proxy.username, proxy.password)
            timeout = aiohttp.ClientTimeout(total=PROXY_TIMEOUT)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    PROXY_TEST_URL,
                    proxy=proxy.server,
                    proxy_auth=auth,
                    ssl=False
                ) as response:
                    if response.status == 200:
                        logging.info(f"Proxy {proxy.server} is working.")
                        return True
                    else:
                        logging.warning(f"Proxy {proxy.server} test failed with status: {response.status}")
                        return False
        except Exception as e:
            logging.warning(f"Proxy {proxy.server} test failed: {type(e).__name__}")
            return False
    
    async def initialize(self, sample_size: int = 200):
        """Test a sample of proxies to find working ones"""
        if not self.proxies:
            raise Exception("Proxy list is empty or contains no valid proxies.")

        logging.info(f"Testing a random sample of {min(sample_size, len(self.proxies))} proxies...")
        sample = random.sample(self.proxies, min(sample_size, len(self.proxies)))
        
        # Test in smaller batches to avoid overwhelming
        batch_size = 50
        self.working_proxies = []
        
        for i in range(0, len(sample), batch_size):
            batch = sample[i:i + batch_size]
            tasks = [self.test_proxy(proxy) for proxy in batch]
            results = await asyncio.gather(*tasks)
            
            working_batch = [
                proxy for proxy, is_working in zip(batch, results) if is_working
            ]
            self.working_proxies.extend(working_batch)
            
            logging.info(f"Batch {i//batch_size + 1}: Found {len(working_batch)} working proxies")
            
            # Stop if we have enough working proxies
            if len(self.working_proxies) >= 100:
                break
        
        logging.info(f"Total found {len(self.working_proxies)} working proxies out of {len(sample)} tested.")
        
        if not self.working_proxies:
            raise Exception("No working proxies found from the tested sample!")
    
    async def get_proxy(self) -> Optional[Proxy]:
        """Get next working proxy"""
        async with self.lock:
            if not self.working_proxies:
                return None
                
            proxy = self.working_proxies[self.current_index % len(self.working_proxies)]
            self.current_index += 1
            
            if proxy.failures > 3:
                self.working_proxies.remove(proxy)
                logging.warning(f"Removed consistently failing proxy: {proxy.server}")
                return await self.get_proxy() if self.working_proxies else None
                
            return proxy
    
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
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('completed', []))
            except json.JSONDecodeError:
                logging.warning(f"Could not read checkpoint file {self.checkpoint_file}. Starting fresh.")
                return set()
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
    proxy_manager: ProxyManager,
    use_proxy: bool = True
) -> Optional[Dict]:
    """Scrape hotels for a specific city and date"""
    
    for attempt in range(MAX_RETRIES):
        proxy = await proxy_manager.get_proxy() if use_proxy else None
        if use_proxy and not proxy:
            logging.error("No working proxies available for this attempt.")
            return None
            
        browser = None
        try:
            async with async_playwright() as p:
                launch_options = {
                    "headless": True,
                    "args": ['--disable-blink-features=AutomationControlled']
                }
                if use_proxy and proxy:
                    launch_options["proxy"] = {
                        "server": proxy.server,
                        "username": proxy.username,
                        "password": proxy.password
                    }
                
                browser = await p.chromium.launch(**launch_options)
                
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                )
                
                page = await context.new_page()
                
                await page.goto("https://www.aadvantagehotels.com/", timeout=PAGE_TIMEOUT)
                await asyncio.sleep(random.uniform(2, 4))
                
                city_input = await page.wait_for_selector('input[placeholder="Enter a city, airport, or landmark"]', timeout=15000)
                await city_input.click()
                await asyncio.sleep(1)
                await city_input.fill(city)
                await asyncio.sleep(2)
                
                dropdown_options = await page.query_selector_all('ul[role="listbox"] li')
                if dropdown_options:
                    await dropdown_options[0].click()
                else:
                    await page.keyboard.press('ArrowDown')
                    await page.keyboard.press('Enter')
                await asyncio.sleep(1)
                
                checkin_str = checkin.strftime("%m/%d/%Y")
                checkout_str = checkout.strftime("%m/%d/%Y")
                
                await page.evaluate(f'document.querySelector(\'input[placeholder="Check-in"]\').value = "{checkin_str}"')
                await page.evaluate(f'document.querySelector(\'input[placeholder="Check-out"]\').value = "{checkout_str}"')
                await asyncio.sleep(1)
                
                # Click search and wait for either navigation OR content change
                search_button = await page.get_by_role("button", name="Search")
                
                # Instead of waiting for navigation, wait for results to appear
                try:
                    # Click and wait for either navigation or hotel results
                    await search_button.click()
                    
                    # Wait for EITHER URL change OR hotel cards to appear
                    await page.wait_for_function(
                        """() => {
                            // Check if URL contains search
                            if (window.location.href.includes('search')) return true;
                            // Check if hotel cards are present
                            if (document.querySelector('[data-testid="hotel-name"]')) return true;
                            if (document.querySelector('[data-testid^="hotel-card-"]')) return true;
                            // Check for any results container
                            if (document.querySelector('.hotel-results')) return true;
                            if (document.querySelector('#results')) return true;
                            return false;
                        }""",
                        timeout=45000
                    )
                    
                    logging.info(f"Search completed for {city}")
                    await asyncio.sleep(3)  # Let results fully load
                    
                except Exception as e:
                    logging.warning(f"Search might have failed: {e}")
                    # Continue anyway in case results loaded
                
                # Check current URL
                current_url = page.url
                logging.debug(f"Current URL: {current_url}")
                
                # Try to sort by miles if we're on a search page
                if "search" in current_url or "results" in current_url:
                    if "sort=" not in current_url:
                        sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
                    else:
                        sorted_url = re.sub(r'sort=[^&]*', 'sort=milesHighest', current_url)
                    
                    try:
                        await page.goto(sorted_url, timeout=PAGE_TIMEOUT)
                        await asyncio.sleep(2)
                    except:
                        logging.debug("Could not navigate to sorted URL")
                
                # Look for hotel cards with multiple possible selectors
                hotel_selectors = [
                    'div[data-testid^="hotel-card-"]',
                    '[data-testid="hotel-card"]',
                    '.hotel-card',
                    '.property-card',
                    'article[class*="hotel"]',
                    'div[class*="property"]'
                ]
                
                hotel_elements = []
                for selector in hotel_selectors:
                    hotel_elements = await page.query_selector_all(selector)
                    if hotel_elements:
                        logging.info(f"Found {len(hotel_elements)} hotels with selector: {selector}")
                        break
                
                if not hotel_elements:
                    logging.warning(f"No hotel cards found for {city}")
                    # Take a screenshot for debugging
                    await page.screenshot(path=f"no_hotels_{city.replace(',', '').replace(' ', '_')}.png")
                    return None
                
                cheapest_10k = None
                
                for card in hotel_elements[:20]:
                    try:
                        # Try multiple selectors for hotel details
                        name_elem = await card.query_selector('[data-testid="hotel-name"]') or \
                                   await card.query_selector('.hotel-name') or \
                                   await card.query_selector('h3') or \
                                   await card.query_selector('[class*="property-name"]')
                        
                        price_elem = await card.query_selector('[data-testid="earn-price"]') or \
                                    await card.query_selector('[class*="price"]') or \
                                    await card.query_selector('[data-testid="price"]')
                        
                        miles_elem = await card.query_selector('[data-testid="tier-earn-rewards"]') or \
                                    await card.query_selector('[class*="miles"]') or \
                                    await card.query_selector('[class*="points"]')

                        if not (name_elem and price_elem and miles_elem):
                            continue

                        name = await name_elem.text_content()
                        price_text = await price_elem.text_content()
                        miles_text = await miles_elem.text_content()
                        
                        price_match = re.search(r'([\d,]+\.?\d*)', price_text)
                        price = float(price_match.group(1).replace(',', '')) if price_match else None
                        
                        miles_numbers = re.findall(r'([\d,]+)', miles_text)
                        points = max([int(n.replace(',', '')) for n in miles_numbers if n] or [0])

                        if name and price and points == 10000:
                            hotel_data = {
                                'City': city,
                                'Hotel': name.strip(),
                                'Price': price,
                                'Points': points,
                                'Check-in': checkin.strftime("%Y-%m-%d"),
                                'Check-out': checkout.strftime("%Y-%m-%d"),
                                'Cost per Point': price / points if points > 0 else 0
                            }
                            if not cheapest_10k or price < cheapest_10k['Price']:
                                cheapest_10k = hotel_data
                                
                    except Exception as e:
                        logging.debug(f"Error parsing hotel card: {e}")
                        continue
                
                return cheapest_10k
                
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} for {city} on {checkin.date()} failed: {type(e).__name__}: {str(e)}")
            if use_proxy and proxy:
                await proxy_manager.mark_proxy_failed(proxy)
            
        finally:
            if browser:
                await browser.close()
                
        await asyncio.sleep(random.uniform(3, 6))
    
    return None

async def scrape_city(
    city: str,
    proxy_manager: ProxyManager,
    checkpoint_manager: CheckpointManager,
    results_manager,
    use_proxy: bool
) -> Optional[Dict]:
    """Scrape all dates for a city"""
    
    if checkpoint_manager.is_completed(city):
        logging.info(f"Skipping {city} - already completed")
        return None
    
    logging.info(f"Scraping {city}...")
    city_cheapest = None
    
    for day in range(NUM_DATES):
        checkin = datetime.now() + timedelta(days=day + 1)
        checkout = checkin + timedelta(days=NUM_NIGHTS)
        
        result = await scrape_city_date(city, checkin, checkout, proxy_manager, use_proxy)
        
        if result and (not city_cheapest or result['Price'] < city_cheapest['Price']):
            city_cheapest = result
        
        await asyncio.sleep(random.uniform(1, 3))
    
    if city_cheapest:
        await results_manager.add_result(city_cheapest)
        logging.info(f"✅ Found 10K hotel in {city}: {city_cheapest['Hotel']} - ${city_cheapest['Price']}")
    else:
        logging.info(f"❌ No 10K hotels found in {city}")
    
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
            try:
                df = pd.read_csv(self.results_file)
                self.results = df.to_dict('records')
                logging.info(f"Loaded {len(self.results)} existing results")
            except pd.errors.EmptyDataError:
                logging.warning(f"Results file {self.results_file} is empty.")
                self.results = []
    
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
    use_proxy: bool,
    worker_id: int
):
    """Worker coroutine for processing cities"""
    while True:
        try:
            city = await cities_queue.get()
            if city is None:
                break
                
            logging.info(f"Worker {worker_id} processing {city}")
            await scrape_city(city, proxy_manager, checkpoint_manager, results_manager, use_proxy)
            
        except Exception as e:
            logging.error(f"Worker {worker_id} had a fatal error: {e}", exc_info=True)
        finally:
            cities_queue.task_done()

async def main(
    cities_file: str,
    proxy_file: str,
    batch_start: int,
    batch_size: Optional[int],
    max_concurrent: int,
    no_proxy: bool
):
    """Main scraping function"""
    
    with open(cities_file, 'r') as f:
        all_cities = [line.strip() for line in f if line.strip()]
    
    cities_in_batch = all_cities
    if batch_size is not None:
        cities_in_batch = all_cities[batch_start : batch_start + batch_size]
        logging.info(f"Processing batch: {len(cities_in_batch)} cities starting from index {batch_start}")
    
    proxy_manager = ProxyManager(proxy_file)
    if not no_proxy:
        try:
            await proxy_manager.initialize()
        except Exception as e:
            logging.error(f"Failed to initialize Proxy Manager: {e}")
            return

    checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)
    results_manager = ResultsManager(RESULTS_FILE)
    
    cities_to_process = [c for c in cities_in_batch if not checkpoint_manager.is_completed(c)]
    logging.info(f"Cities to process in this run: {len(cities_to_process)}")
    
    if not cities_to_process:
        logging.info("All cities in this batch are already completed!")
        return
    
    cities_queue = asyncio.Queue()
    for city in cities_to_process:
        await cities_queue.put(city)
    
    for _ in range(max_concurrent):
        await cities_queue.put(None)
    
    workers = [
        asyncio.create_task(
            worker(cities_queue, proxy_manager, checkpoint_manager, results_manager, not no_proxy, i)
        )
        for i in range(max_concurrent)
    ]
    
    await cities_queue.join()

    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    
    logging.info(f"Scraping complete! Total results in memory: {len(results_manager.results)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AA Hotels Scraper")
    parser.add_argument("--cities", default="cities_top200.txt", help="Cities file")
    parser.add_argument("--proxies", default="formatted_proxies.txt", help="Proxies file")
    parser.add_argument("--batch-start", type=int, default=0, help="Batch start index")
    parser.add_argument("--batch-size", type=int, default=None, help="Batch size")
    parser.add_argument("--concurrent", type=int, default=MAX_CONCURRENT, help="Max concurrent workers")
    parser.add_argument("--no-proxy", action="store_true", help="Run the scraper without using proxies.")
    
    args = parser.parse_args()
    
    asyncio.run(main(
        cities_file=args.cities,
        proxy_file=args.proxies,
        batch_start=args.batch_start,
        batch_size=args.batch_size,
        max_concurrent=args.concurrent,
        no_proxy=args.no_proxy
    ))

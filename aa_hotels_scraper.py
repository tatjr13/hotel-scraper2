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
        """Initialize proxies - for WebShare, skip testing as they often fail tests but work in practice"""
        if not self.proxies:
            raise Exception("Proxy list is empty or contains no valid proxies.")

        logging.info(f"Loading {len(self.proxies)} proxies...")
        
        # For WebShare proxies, don't test them - just use them
        # WebShare proxies often fail basic tests but work for actual browsing
        if self.proxies and 'webshare' in self.proxies[0].server:
            logging.info("Detected WebShare proxies - skipping test phase")
            # Just use a random sample of proxies
            self.working_proxies = random.sample(self.proxies, min(100, len(self.proxies)))
            logging.info(f"Using {len(self.working_proxies)} WebShare proxies without testing")
            return
        
        # For other proxy providers, test as normal
        logging.info(f"Testing a random sample of {min(sample_size, len(self.proxies))} proxies...")
        sample = random.sample(self.proxies, min(sample_size, len(self.proxies)))
        
        # Test in smaller batches
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
            logging.warning("No working proxies found from tests, using untested proxies")
            self.working_proxies = sample[:50]
    
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
                
                # Fill in the city
                city_input = await page.wait_for_selector('input[placeholder="Enter a city, airport, or landmark"]', timeout=15000)
                await city_input.click()
                await asyncio.sleep(1)
                await city_input.fill(city)
                await asyncio.sleep(2)
                
                # Select from dropdown
                dropdown_options = await page.query_selector_all('ul[role="listbox"] li')
                if dropdown_options:
                    await dropdown_options[0].click()
                    logging.info(f"Selected {city} from dropdown")
                else:
                    await page.keyboard.press('ArrowDown')
                    await page.keyboard.press('Enter')
                await asyncio.sleep(1)
                
                # Handle dates properly - THIS IS THE CRITICAL PART
                checkin_str = checkin.strftime("%m/%d/%Y")
                checkout_str = checkout.strftime("%m/%d/%Y")
                
                logging.info(f"Setting dates: Check-in: {checkin_str}, Check-out: {checkout_str}")
                
                # Set check-in date
                checkin_input = await page.wait_for_selector('input[placeholder="Check-in"]')
                await checkin_input.click()
                await asyncio.sleep(0.5)
                
                # Clear and type check-in date
                await page.keyboard.press('Control+A')  # Select all
                await checkin_input.type(checkin_str)
                await asyncio.sleep(0.5)
                
                # Move to check-out field
                await page.keyboard.press('Tab')
                await asyncio.sleep(0.5)
                
                # Set check-out date
                checkout_input = await page.wait_for_selector('input[placeholder="Check-out"]')
                await page.keyboard.press('Control+A')  # Select all
                await checkout_input.type(checkout_str)
                await asyncio.sleep(0.5)
                
                # Close any calendar by pressing Escape
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)
                
                # Verify all fields are filled
                city_value = await city_input.get_attribute('value')
                checkin_value = await checkin_input.get_attribute('value')
                checkout_value = await checkout_input.get_attribute('value')
                
                logging.info(f"Form values - City: {city_value}, Check-in: {checkin_value}, Check-out: {checkout_value}")
                
                # Make sure dates are different
                if checkin_value == checkout_value:
                    logging.warning("Dates are the same, retrying checkout...")
                    await checkout_input.click()
                    await page.keyboard.press('Control+A')
                    await checkout_input.type(checkout_str)
                    await page.keyboard.press('Enter')
                    await asyncio.sleep(1)
                
                # Click search button
                search_button = await page.wait_for_selector('button:has-text("Search")', state='visible')
                await search_button.click()
                logging.info("Clicked search button")
                
                # Wait for navigation or results
                old_url = page.url
                try:
                    # Wait for URL change or results to appear
                    await page.wait_for_function(
                        f"""() => {{
                            return window.location.href !== '{old_url}' || 
                                   document.querySelector('[data-testid*="hotel"]') !== null ||
                                   document.querySelector('.hotel-card') !== null ||
                                   document.querySelector('.property-card') !== null;
                        }}""",
                        timeout=30000
                    )
                    logging.info("Page changed or results appeared")
                except:
                    logging.warning("Timeout waiting for results")
                
                await asyncio.sleep(3)
                
                current_url = page.url
                logging.info(f"Current URL: {current_url}")
                
                # Try to sort by miles if on search page
                if current_url != old_url and "sort=" not in current_url:
                    sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
                    try:
                        await page.goto(sorted_url, timeout=PAGE_TIMEOUT)
                        await asyncio.sleep(2)
                        logging.info("Applied sort by miles")
                    except:
                        pass
                
                # Look for hotel cards
                hotel_elements = []
                hotel_selectors = [
                    'div[data-testid^="hotel-card-"]',
                    '[data-testid="hotel-card"]',
                    '[data-testid*="property-card"]',
                    'div[class*="HotelCard"]',
                    'div[class*="PropertyCard"]',
                    'article[class*="hotel"]',
                    'div[class*="hotel-card"]',
                    'div[class*="property-card"]',
                    '.hotel-item',
                    '.property-item'
                ]
                
                for selector in hotel_selectors:
                    hotel_elements = await page.query_selector_all(selector)
                    if hotel_elements:
                        logging.info(f"Found {len(hotel_elements)} hotels with selector: {selector}")
                        break
                
                if not hotel_elements:
                    logging.warning(f"No hotel cards found for {city}")
                    await page.screenshot(path=f"no_hotels_{city.replace(',', '').replace(' ', '_')}_{attempt}.png", full_page=True)
                    continue
                
                cheapest_10k = None
                
                # Parse hotel cards
                for card in hotel_elements[:20]:
                    try:
                        # Look for hotel name
                        name_elem = None
                        name_selectors = [
                            '[data-testid="hotel-name"]',
                            '[data-testid="property-name"]',
                            '.hotel-name',
                            '.property-name',
                            'h2',
                            'h3',
                            '[class*="hotel-name"]',
                            '[class*="property-name"]'
                        ]
                        
                        for sel in name_selectors:
                            name_elem = await card.query_selector(sel)
                            if name_elem:
                                break
                        
                        # Look for price
                        price_elem = None
                        price_selectors = [
                            '[data-testid="earn-price"]',
                            '[data-testid="price"]',
                            '[class*="price"]',
                            '[class*="rate"]',
                            'span:has-text("$")'
                        ]
                        
                        for sel in price_selectors:
                            price_elem = await card.query_selector(sel)
                            if price_elem:
                                break
                        
                        # Look for miles/points
                        miles_elem = None
                        miles_selectors = [
                            '[data-testid="tier-earn-rewards"]',
                            '[data-testid="points"]',
                            '[data-testid="miles"]',
                            '[class*="miles"]',
                            '[class*="points"]',
                            'span:has-text("miles")',
                            'span:has-text("points")'
                        ]
                        
                        for sel in miles_selectors:
                            miles_elem = await card.query_selector(sel)
                            if miles_elem:
                                break

                        if not (name_elem and price_elem and miles_elem):
                            continue

                        name = await name_elem.text_content()
                        price_text = await price_elem.text_content()
                        miles_text = await miles_elem.text_content()
                        
                        # Extract price
                        price_match = re.search(r'([\d,]+\.?\d*)', price_text)
                        price = float(price_match.group(1).replace(',', '')) if price_match else None
                        
                        # Extract points
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
                                logging.info(f"Found 10K hotel: {name.strip()} at ${price}")
                                
                    except Exception as e:
                        logging.debug(f"Error parsing hotel card: {e}")
                        continue
                
                if cheapest_10k:
                    return cheapest_10k
                else:
                    logging.info(f"No 10K hotels found in results for {city}")
                    
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

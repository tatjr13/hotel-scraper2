import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import re
import random
import os
import json
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProxyManager:
    def __init__(self, proxy_file='formatted_proxies.txt'):
        self.proxy_file = proxy_file
        self.proxies = []
        self.working_proxies = []
        self.current_index = 0
        self.lock = asyncio.Lock()
        self.failed_proxies = set()
        
    async def initialize(self, use_proxies=True):
        if not use_proxies:
            logging.info("Running without proxies")
            return
            
        logging.info(f"Attempting to load proxies from {self.proxy_file}...")
        
        try:
            with open(self.proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            
            if not self.proxies:
                logging.warning("No proxies found in file")
                return
                
            logging.info(f"Successfully loaded {len(self.proxies)} proxies.")
            
            # For WebShare proxies, use a subset for rotation
            # Take every Nth proxy to get diversity across different subnets
            step = max(1, len(self.proxies) // 1000)  # Get ~1000 diverse proxies
            self.working_proxies = self.proxies[::step][:1000]
            
            logging.info(f"Selected {len(self.working_proxies)} diverse proxies for rotation")
                
        except FileNotFoundError:
            logging.error(f"Proxy file {self.proxy_file} not found")
        except Exception as e:
            logging.error(f"Error loading proxies: {str(e)}")
    
    def parse_proxy_url(self, proxy_url):
        """Convert proxy URL to Playwright proxy configuration"""
        if not proxy_url:
            return None
            
        try:
            # Remove http:// prefix
            url_without_protocol = proxy_url.replace('http://', '').replace('https://', '')
            
            # Split auth and server parts
            auth_part, server_part = url_without_protocol.split('@', 1)
            username, password = auth_part.split(':', 1)
            
            return {
                "server": f"http://{server_part}",
                "username": username,
                "password": password
            }
                
        except Exception as e:
            logging.error(f"Error parsing proxy {proxy_url}: {str(e)}")
            return None
    
    async def get_proxy(self):
        """Get next working proxy with rotation"""
        async with self.lock:
            if not self.working_proxies:
                return None
            
            # Skip failed proxies
            attempts = 0
            while attempts < len(self.working_proxies):
                proxy = self.working_proxies[self.current_index % len(self.working_proxies)]
                self.current_index += 1
                attempts += 1
                
                if proxy not in self.failed_proxies:
                    return proxy
            
            # If all proxies have failed, reset the failed set and try again
            logging.warning("All proxies have failed, resetting failed proxy list")
            self.failed_proxies.clear()
            return self.working_proxies[0] if self.working_proxies else None
    
    async def mark_proxy_failed(self, proxy):
        """Mark a proxy as failed"""
        async with self.lock:
            self.failed_proxies.add(proxy)
            logging.info(f"Marked proxy as failed. {len(self.failed_proxies)} failed out of {len(self.working_proxies)} total.")

async def create_browser_context(playwright, proxy_manager, use_proxy=True):
    """Create a browser context with or without proxy"""
    browser = None
    
    try:
        if use_proxy and proxy_manager.working_proxies:
            # Try up to 5 different proxies
            for attempt in range(5):
                proxy_url = await proxy_manager.get_proxy()
                if not proxy_url:
                    break
                    
                proxy_config = proxy_manager.parse_proxy_url(proxy_url)
                if not proxy_config:
                    continue
                
                try:
                    # Launch browser with proxy
                    browser = await playwright.chromium.launch(
                        headless=True,
                        proxy=proxy_config,
                        args=[
                            '--disable-blink-features=AutomationControlled',
                            '--disable-features=IsolateOrigins,site-per-process',
                            '--disable-web-security',
                            '--disable-setuid-sandbox',
                            '--no-sandbox'
                        ]
                    )
                    
                    # Create context with additional stealth settings
                    context = await browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                        locale='en-US',
                        timezone_id='America/New_York'
                    )
                    
                    # Test the proxy with a simple request
                    page = await context.new_page()
                    try:
                        await page.goto('https://httpbin.org/ip', timeout=15000)
                        content = await page.content()
                        if '"origin"' in content:
                            ip = re.search(r'"origin":\s*"([^"]+)"', content)
                            if ip:
                                logging.info(f"Proxy working! IP: {ip.group(1)}")
                            await page.close()
                            return browser, context
                    except Exception as e:
                        await page.close()
                        await context.close()
                        await browser.close()
                        await proxy_manager.mark_proxy_failed(proxy_url)
                        logging.warning(f"Proxy test failed (attempt {attempt + 1}/5): {str(e)}")
                        continue
                        
                except Exception as e:
                    if browser:
                        await browser.close()
                    await proxy_manager.mark_proxy_failed(proxy_url)
                    logging.warning(f"Browser launch failed with proxy (attempt {attempt + 1}/5): {str(e)}")
        
        # If no proxy or all proxies failed, launch without proxy
        logging.info("Launching browser without proxy")
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York'
        )
        
        return browser, context
        
    except Exception as e:
        logging.error(f"Failed to create browser: {str(e)}")
        if browser:
            await browser.close()
        raise

async def scrape_city_with_retry(city, dates_to_check, proxy_manager, max_retries=3):
    """Scrape a city with retry logic and proxy rotation"""
    
    for retry in range(max_retries):
        try:
            async with async_playwright() as p:
                browser, context = await create_browser_context(p, proxy_manager, use_proxy=True)
                
                try:
                    # Scrape with current browser/proxy
                    result = await scrape_city(browser, context, city, dates_to_check)
                    return result
                    
                except Exception as e:
                    logging.error(f"Scraping failed for {city} (attempt {retry + 1}/{max_retries}): {str(e)}")
                    
                    # Check if it's an error page
                    if "something went wrong" in str(e).lower():
                        logging.warning("Detected 'something went wrong' error - likely bot detection")
                    
                finally:
                    await context.close()
                    await browser.close()
                    
        except Exception as e:
            logging.error(f"Browser creation failed (attempt {retry + 1}/{max_retries}): {str(e)}")
        
        # Wait before retry
        await asyncio.sleep(random.uniform(5, 10))
    
    return None

async def scrape_city(browser, context, city, dates_to_check):
    """Scrape hotels for a specific city"""
    all_results = []
    
    for checkin_date, checkout_date in dates_to_check:
        logging.info(f"Checking {city} for {checkin_date} to {checkout_date}")
        
        page = None
        try:
            page = await context.new_page()
            
            # Add stealth scripts
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            
            # Navigate to the website
            await page.goto("https://www.aadvantagehotels.com/", wait_until='networkidle', timeout=30000)
            await asyncio.sleep(random.uniform(2, 4))
            
            # Check for error page
            content = await page.content()
            if "something went wrong" in content.lower() or "error" in await page.title():
                raise Exception("Error page detected - possible bot detection")
            
            # Fill in search form
            # City input
            city_input = await page.wait_for_selector('input[placeholder="Enter a city, airport, or landmark"]', timeout=15000)
            await city_input.click()
            await asyncio.sleep(random.uniform(0.5, 1))
            
            # Type slowly to appear more human
            for char in city:
                await city_input.type(char, delay=random.randint(50, 150))
            await asyncio.sleep(random.uniform(1, 2))
            
            # Select from dropdown
            try:
                await page.wait_for_selector('ul[role="listbox"] li', timeout=5000)
                dropdown_options = await page.query_selector_all('ul[role="listbox"] li')
                if dropdown_options:
                    await dropdown_options[0].click()
                else:
                    await page.keyboard.press('ArrowDown')
                    await asyncio.sleep(0.5)
                    await page.keyboard.press('Enter')
            except:
                await page.keyboard.press('ArrowDown')
                await asyncio.sleep(0.5)
                await page.keyboard.press('Enter')
            
            await asyncio.sleep(random.uniform(1, 2))
            
            # Fill dates
            checkin_input = await page.wait_for_selector('input[placeholder="Check-in"]', timeout=10000)
            await checkin_input.click()
            await checkin_input.fill(checkin_date, force=True)
            await asyncio.sleep(random.uniform(0.5, 1))
            
            checkout_input = await page.wait_for_selector('input[placeholder="Check-out"]', timeout=10000)
            await checkout_input.click()
            await checkout_input.fill(checkout_date, force=True)
            await asyncio.sleep(random.uniform(0.5, 1))
            
            # Click search
            search_button = await page.get_by_role("button", name="Search")
            await search_button.click()
            
            # Wait for navigation
            try:
                await page.wait_for_url("**/search**", timeout=30000)
            except:
                current_url = page.url
                if "/search" not in current_url:
                    logging.warning(f"Failed to navigate to search results. Current URL: {current_url}")
                    continue
            
            await asyncio.sleep(random.uniform(3, 5))
            
            # Check for error on results page
            content = await page.content()
            if "something went wrong" in content.lower():
                raise Exception("Error page on search results")
            
            # Sort by highest miles
            current_url = page.url
            if "sort=" not in current_url:
                sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
            else:
                sorted_url = re.sub(r'sort=[^&]*', 'sort=milesHighest', current_url)
            
            await page.goto(sorted_url, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(random.uniform(3, 5))
            
            # Parse results
            hotels = await parse_hotel_results(page, city, checkin_date, checkout_date)
            all_results.extend(hotels)
            
        except Exception as e:
            logging.error(f"Error scraping {city} for {checkin_date}: {str(e)}")
            
        finally:
            if page and not page.is_closed():
                await page.close()
            
            # Random delay between searches
            await asyncio.sleep(random.uniform(5, 10))
    
    return all_results

async def parse_hotel_results(page, city, checkin_date, checkout_date):
    """Parse hotel results from the search page"""
    results = []
    
    try:
        # Wait for hotel cards to load
        await page.wait_for_selector('[data-testid="hotel-name"]', timeout=15000)
        hotel_cards = await page.query_selector_all('[data-testid="hotel-name"]')
        
        logging.info(f"Found {len(hotel_cards)} hotels on page")
        
        for i, name_elem in enumerate(hotel_cards[:30]):  # Check up to 30 hotels
            try:
                name = await name_elem.text_content()
                
                # Get parent card
                card = await name_elem.evaluate_handle('''el => {
                    return el.closest('[data-testid*="hotel"]') || 
                           el.closest('div').parentElement.parentElement;
                }''')
                
                if not card:
                    continue
                
                # Extract price
                price = None
                price_elem = await card.query_selector('[data-testid="earn-price"]')
                if price_elem:
                    price_text = await price_elem.text_content()
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace('$', ''))
                    if price_match:
                        price = float(price_match.group().replace(',', ''))
                
                # Extract points
                points = None
                miles_elem = await card.query_selector('[data-testid="tier-earn-rewards"]')
                if miles_elem:
                    miles_text = await miles_elem.text_content()
                    # Look for patterns like "10,000 miles" or "Earn 10,000"
                    miles_numbers = re.findall(r'([\d,]+)\s*(?:miles|points)?', miles_text, re.IGNORECASE)
                    if miles_numbers:
                        all_numbers = [int(n.replace(',', '')) for n in miles_numbers]
                        points = max(all_numbers)  # Take the highest number as points
                
                if name and price and points:
                    # Only include 10,000 point hotels
                    if points == 10000:
                        hotel_data = {
                            'City': city,
                            'Hotel': name.strip(),
                            'Price': price,
                            'Points': points,
                            'Check-in': checkin_date,
                            'Check-out': checkout_date,
                            'Cost per Point': price / points
                        }
                        results.append(hotel_data)
                        logging.info(f"Found 10K hotel: {name.strip()} - ${price}")
                
            except Exception as e:
                logging.debug(f"Error parsing hotel {i}: {str(e)}")
                continue
        
        logging.info(f"Found {len(results)} hotels with 10,000 points")
        
    except Exception as e:
        logging.error(f"Error parsing results: {str(e)}")
    
    return results

async def main():
    # Load cities
    cities_file = os.getenv('CITIES_FILE', 'cities_top200.txt')
    try:
        with open(cities_file, 'r') as f:
            all_cities = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"Cities file {cities_file} not found!")
        return
    
    # Get batch parameters
    batch_start = int(os.getenv('BATCH_START', 0))
    batch_size = int(os.getenv('BATCH_SIZE', 10))
    
    cities = all_cities[batch_start:batch_start + batch_size]
    
    logging.info(f"Processing batch: {len(cities)} cities starting from index {batch_start}")
    
    # Initialize proxy manager
    proxy_manager = ProxyManager()
    use_proxies = os.getenv('NO_PROXY', '').lower() != 'true'
    await proxy_manager.initialize(use_proxies=use_proxies)
    
    # Generate dates to check
    dates_to_check = []
    for days_ahead in range(1, 4):  # Check 3 different dates
        checkin = datetime.now() + timedelta(days=days_ahead)
        checkout = checkin + timedelta(days=1)
        dates_to_check.append((
            checkin.strftime("%m/%d/%Y"),
            checkout.strftime("%m/%d/%Y")
        ))
    
    # Process cities with concurrent workers
    num_workers = int(os.getenv('NUM_WORKERS', 3))
    semaphore = asyncio.Semaphore(num_workers)
    
    async def process_city_with_semaphore(city):
        async with semaphore:
            logging.info(f"Starting to scrape {city}")
            results = await scrape_city_with_retry(city, dates_to_check, proxy_manager)
            
            if results:
                # Find cheapest 10K hotel for this city
                cheapest = min(results, key=lambda x: x['Price'])
                logging.info(f"✅ Cheapest 10K hotel in {city}: {cheapest['Hotel']} - ${cheapest['Price']}")
                return cheapest
            else:
                logging.warning(f"❌ No 10K hotels found in {city}")
                return None
    
    # Process all cities concurrently
    tasks = [process_city_with_semaphore(city) for city in cities]
    results = await asyncio.gather(*tasks)
    
    # Filter out None results
    valid_results = [r for r in results if r is not None]
    
    # Save results
    if valid_results:
        df = pd.DataFrame(valid_results)
        output_file = f"cheapest_10k_hotels_batch_{batch_start}.csv"
        df.to_csv(output_file, index=False)
        logging.info(f"✅ Saved {len(valid_results)} results to {output_file}")
        
        # Also append to master file
        master_file = "cheapest_10k_hotels_all.csv"
        if os.path.exists(master_file):
            existing_df = pd.read_csv(master_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(master_file, index=False)
        else:
            df.to_csv(master_file, index=False)
        
        logging.info(f"✅ Updated master file: {master_file}")
    else:
        logging.warning("No valid results found in this batch")

if __name__ == "__main__":
    asyncio.run(main())

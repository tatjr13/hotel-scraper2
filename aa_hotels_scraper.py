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
        self.current_index = 0
        
    def load_proxies(self):
        """Load proxies from file"""
        try:
            with open(self.proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logging.info(f"Loaded {len(self.proxies)} proxies")
            return True
        except FileNotFoundError:
            logging.warning(f"Proxy file {self.proxy_file} not found")
            return False
    
    def get_next_proxy(self):
        """Get next proxy in rotation"""
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index % len(self.proxies)]
        self.current_index += 1
        return proxy
    
    def parse_proxy(self, proxy_url):
        """Parse proxy URL to Playwright format"""
        try:
            # Remove http:// prefix
            clean_url = proxy_url.replace('http://', '').replace('https://', '')
            
            # Split into auth and server parts
            auth_part, server_part = clean_url.split('@', 1)
            username, password = auth_part.split(':', 1)
            
            return {
                "server": f"http://{server_part}",
                "username": username,
                "password": password
            }
        except Exception as e:
            logging.error(f"Error parsing proxy {proxy_url}: {str(e)}")
            return None

async def create_browser_with_proxy(playwright, proxy_manager):
    """Create browser with proxy from pool"""
    proxy_url = proxy_manager.get_next_proxy()
    
    browser_args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-images',  # Save bandwidth
        '--disable-plugins',
        '--disable-java'
    ]
    
    if proxy_url:
        proxy_config = proxy_manager.parse_proxy(proxy_url)
        if proxy_config:
            logging.info(f"Using proxy #{proxy_manager.current_index}")
            browser = await playwright.chromium.launch(
                headless=True,
                proxy=proxy_config,
                args=browser_args
            )
        else:
            logging.warning("Failed to parse proxy, running without proxy")
            browser = await playwright.chromium.launch(headless=True, args=browser_args)
    else:
        logging.info("No proxy available, running without proxy")
        browser = await playwright.chromium.launch(headless=True, args=browser_args)
    
    return browser

async def create_context(browser):
    """Create browser context with stealth settings"""
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        locale='en-US',
        timezone_id='America/New_York'
    )
    
    # Add stealth script
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
    """)
    
    # Block unnecessary resources to save bandwidth
    await context.route('**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,eot}', lambda route: route.abort())
    await context.route('**/analytics/**', lambda route: route.abort())
    await context.route('**/google-analytics.com/**', lambda route: route.abort())
    await context.route('**/googletagmanager.com/**', lambda route: route.abort())
    await context.route('**/facebook.com/**', lambda route: route.abort())
    await context.route('**/doubleclick.net/**', lambda route: route.abort())
    
    return context

async def scrape_city(page, city, dates_to_check):
    """Scrape hotels for a specific city and dates"""
    all_results = []
    
    for date_index, (checkin_date, checkout_date) in enumerate(dates_to_check):
        logging.info(f"  Checking {checkin_date} to {checkout_date}")
        
        try:
            # Navigate to homepage for first date only
            if date_index == 0:
                await page.goto("https://www.aadvantagehotels.com/", wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(random.randint(2000, 3000))
                
                # Check for bot detection
                content = await page.content()
                if "something went wrong" in content.lower():
                    logging.error("Bot detection triggered!")
                    return []
                
                # Fill city
                city_input = page.locator('input[placeholder*="city" i], input[placeholder*="City" i]').first
                await city_input.click()
                await page.wait_for_timeout(random.randint(500, 1000))
                await city_input.fill(city)
                await page.wait_for_timeout(random.randint(1500, 2000))
                
                # Select from dropdown
                try:
                    dropdown = page.locator('ul[role="listbox"] li').first
                    await dropdown.click()
                except:
                    await page.keyboard.press('ArrowDown')
                    await page.wait_for_timeout(300)
                    await page.keyboard.press('Enter')
                
                await page.wait_for_timeout(random.randint(1000, 1500))
            
            # Fill dates
            checkin_input = page.locator('input[placeholder*="Check-in" i], input[placeholder*="check-in" i]').first
            await checkin_input.click()
            await page.wait_for_timeout(300)
            await checkin_input.fill(checkin_date)
            
            checkout_input = page.locator('input[placeholder*="Check-out" i], input[placeholder*="check-out" i]').first
            await checkout_input.click()
            await page.wait_for_timeout(300)
            await checkout_input.fill(checkout_date)
            
            await page.wait_for_timeout(random.randint(500, 1000))
            
            # Click search
            search_button = page.locator('button:has-text("Search")').first
            await search_button.click()
            
            # Wait for search results
            try:
                await page.wait_for_url("**/search**", timeout=20000)
            except:
                if "/search" not in page.url:
                    logging.warning("Failed to reach search page")
                    continue
            
            await page.wait_for_timeout(random.randint(3000, 4000))
            
            # Sort by highest miles
            current_url = page.url
            if "sort=" not in current_url:
                sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
                await page.goto(sorted_url, wait_until='domcontentloaded', timeout=20000)
                await page.wait_for_timeout(random.randint(2000, 3000))
            
            # Extract hotels using JavaScript
            hotels = await page.evaluate('''() => {
                const results = [];
                const nameElements = document.querySelectorAll('[data-testid="hotel-name"], [class*="hotel-name"], h3[class*="hotel"], h2[class*="hotel"]');
                
                for (let i = 0; i < Math.min(nameElements.length, 20); i++) {
                    const nameEl = nameElements[i];
                    const name = nameEl.textContent || '';
                    
                    // Find parent card
                    let card = nameEl.closest('[data-testid*="hotel-card"], [class*="hotel-card"], article, [class*="property-card"]');
                    if (!card) {
                        // Go up the tree
                        let current = nameEl;
                        for (let j = 0; j < 5; j++) {
                            current = current.parentElement;
                            if (current && current.textContent.includes('$') && current.textContent.includes('miles')) {
                                card = current;
                                break;
                            }
                        }
                    }
                    
                    if (card) {
                        const cardText = card.textContent || '';
                        
                        // Check for 10,000 points
                        if (cardText.includes('10,000') || cardText.includes('10000')) {
                            // Extract price
                            const priceMatch = cardText.match(/\\$(\\d+(?:,\\d+)?(?:\\.\\d{2})?)/);
                            const price = priceMatch ? parseFloat(priceMatch[1].replace(',', '')) : null;
                            
                            if (name && price) {
                                results.push({
                                    name: name.trim(),
                                    price: price,
                                    points: 10000
                                });
                            }
                        }
                    }
                }
                
                return results;
            }''')
            
            # Process results
            for hotel in hotels:
                all_results.append({
                    'City': city,
                    'Hotel': hotel['name'],
                    'Price': hotel['price'],
                    'Points': hotel['points'],
                    'Check-in': checkin_date,
                    'Check-out': checkout_date,
                    'Cost per Point': hotel['price'] / hotel['points']
                })
            
            logging.info(f"    Found {len(hotels)} hotels with 10K points")
            
            # Go back for next date (saves bandwidth)
            if date_index < len(dates_to_check) - 1:
                await page.go_back()
                await page.wait_for_timeout(random.randint(1500, 2000))
                
        except Exception as e:
            logging.error(f"    Error: {str(e)}")
    
    return all_results

async def main():
    # Load cities from file
    cities_file = 'cities_top200.txt'
    try:
        with open(cities_file, 'r') as f:
            cities = [line.strip() for line in f if line.strip()]
        logging.info(f"Loaded {len(cities)} cities from {cities_file}")
    except FileNotFoundError:
        logging.error(f"Cities file {cities_file} not found!")
        # Fallback cities for testing
        cities = [
            "Bangkok, Thailand",
            "Dubai, United Arab Emirates",
            "London, UK",
            "Singapore",
            "Tokyo, Japan"
        ]
        logging.info(f"Using {len(cities)} fallback cities")
    
    # Initialize proxy manager
    proxy_manager = ProxyManager()
    proxy_manager.load_proxies()
    
    # Generate dates to check (3 dates)
    dates_to_check = []
    for days_ahead in range(1, 4):  # Tomorrow, day after, etc.
        checkin = datetime.now() + timedelta(days=days_ahead)
        checkout = checkin + timedelta(days=1)
        dates_to_check.append((
            checkin.strftime("%m/%d/%Y"),
            checkout.strftime("%m/%d/%Y")
        ))
    
    logging.info(f"Will check {len(dates_to_check)} dates for each city")
    
    all_results = []
    
    async with async_playwright() as p:
        # Process each city
        for i, city in enumerate(cities):
            logging.info(f"\n{'='*60}")
            logging.info(f"Processing city {i+1}/{len(cities)}: {city}")
            logging.info(f"{'='*60}")
            
            # Create new browser for each city (helps avoid detection)
            browser = await create_browser_with_proxy(p, proxy_manager)
            context = await create_context(browser)
            page = await context.new_page()
            
            try:
                # Scrape the city
                city_results = await scrape_city(page, city, dates_to_check)
                
                if city_results:
                    # Find cheapest 10K hotel for this city
                    cheapest = min(city_results, key=lambda x: x['Price'])
                    all_results.append(cheapest)
                    logging.info(f"✅ Cheapest 10K hotel: {cheapest['Hotel']} - ${cheapest['Price']:.2f}")
                    logging.info(f"   Dates: {cheapest['Check-in']} to {cheapest['Check-out']}")
                else:
                    logging.warning(f"❌ No 10K hotels found in {city}")
                
            except Exception as e:
                logging.error(f"Failed to process {city}: {str(e)}")
                
            finally:
                await page.close()
                await context.close()
                await browser.close()
                
                # Delay between cities
                if i < len(cities) - 1:
                    delay = random.uniform(5, 10)
                    logging.info(f"Waiting {delay:.1f}s before next city...")
                    await asyncio.sleep(delay)
    
    # Save results
    if all_results:
        # Sort by price
        all_results.sort(key=lambda x: x['Price'])
        
        # Save to CSV
        df = pd.DataFrame(all_results)
        output_file = "cheapest_10k_hotels.csv"
        df.to_csv(output_file, index=False)
        
        # Print summary
        print(f"\n{'='*60}")
        print("SUMMARY - CHEAPEST 10K HOTELS BY CITY")
        print(f"{'='*60}")
        print(f"Found 10K hotels in {len(all_results)}/{len(cities)} cities\n")
        
        for result in all_results:
            print(f"{result['City']}:")
            print(f"  Hotel: {result['Hotel']}")
            print(f"  Price: ${result['Price']:.2f}")
            print(f"  Cost per point: ${result['Cost per Point']:.4f}")
            print(f"  Dates: {result['Check-in']} to {result['Check-out']}")
            print()
        
        print(f"✅ Results saved to {output_file}")
    else:
        print("\n❌ No 10K hotels found in any city!")

if __name__ == "__main__":
    asyncio.run(main())

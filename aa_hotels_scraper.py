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

async def create_browser(playwright, use_proxy=False):
    """Create browser with or without proxy"""
    browser_args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        '--disable-web-security',
        '--window-size=1920,1080',
        '--start-maximized'
    ]
    browser = await playwright.chromium.launch(
        headless=True,
        args=browser_args
    )
    return browser

async def create_context(browser):
    """Create browser context with anti-detection"""
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        screen={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        locale='en-US',
        timezone_id='America/New_York',
        permissions=['geolocation'],
        geolocation={'latitude': 40.7128, 'longitude': -74.0060},
        device_scale_factor=1,
        has_touch=False
    )
    # Anti-detection script
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'permissions', {
            get: () => ['geolocation', 'notifications']
        });
    """)
    # Block resources to save bandwidth
    await context.route('**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,eot}', lambda route: route.abort())
    await context.route('**/analytics/**', lambda route: route.abort())
    await context.route('**/google-analytics.com/**', lambda route: route.abort())
    await context.route('**/googletagmanager.com/**', lambda route: route.abort())
    return context

async def handle_cloudflare(page):
    """Wait for Cloudflare challenge if present"""
    try:
        title = await page.title()
        if "just a moment" in title.lower():
            logging.info("Cloudflare challenge detected, waiting...")
            for i in range(10):
                await page.wait_for_timeout(2000)
                title = await page.title()
                if "just a moment" not in title.lower() and "aadvantage" in title.lower():
                    logging.info("Cloudflare challenge passed!")
                    return True
            return False
        return True
    except:
        return True

async def scrape_city(context, city, dates_to_check, city_index):
    """Scrape a city across multiple dates"""
    all_results = []
    page = await context.new_page()
    try:
        for date_index, (checkin_date, checkout_date) in enumerate(dates_to_check):
            logging.info(f"Checking {city} for {checkin_date} to {checkout_date}")
            try:
                # Navigate to homepage
                await page.goto("https://www.aadvantagehotels.com/", wait_until='domcontentloaded', timeout=60000)
                await page.wait_for_timeout(3000)

                # Handle Cloudflare
                if not await handle_cloudflare(page):
                    logging.error("Failed to bypass Cloudflare")
                    continue
                await page.wait_for_timeout(2000)

                # Fill search form
                city_selectors = [
                    'input[placeholder*="city" i]',
                    'input[placeholder*="destination" i]',
                    'input[placeholder*="where" i]',
                    'input[type="text"]:not([placeholder*="date"])'
                ]
                city_filled = False
                for selector in city_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=5000)
                        city_input = page.locator(selector).first
                        await city_input.click()
                        await page.keyboard.press('Control+A')
                        await page.keyboard.press('Delete')
                        await city_input.type(city, delay=100)
                        city_filled = True
                        break
                    except:
                        continue
                if not city_filled:
                    logging.error(f"Could not find city input for {city}")
                    continue
                await page.wait_for_timeout(2000)

                # Handle dropdown selection
                try:
                    await page.wait_for_selector('ul[role="listbox"] li, .suggestion', timeout=5000)
                    await page.locator('ul[role="listbox"] li, .suggestion').first.click()
                except:
                    await page.keyboard.press('ArrowDown')
                    await page.wait_for_timeout(500)
                    await page.keyboard.press('Enter')
                await page.wait_for_timeout(1000)

                # Fill dates
                date_filled = False
                date_selectors = [
                    ('input[placeholder*="check-in" i]', 'input[placeholder*="check-out" i]'),
                    ('input[placeholder*="Check-in" i]', 'input[placeholder*="Check-out" i]'),
                    ('input[name*="checkin" i]', 'input[name*="checkout" i]')
                ]
                for checkin_sel, checkout_sel in date_selectors:
                    try:
                        checkin_input = page.locator(checkin_sel).first
                        await checkin_input.click()
                        await checkin_input.fill(checkin_date)
                        checkout_input = page.locator(checkout_sel).first
                        await checkout_input.click()
                        await checkout_input.fill(checkout_date)
                        date_filled = True
                        break
                    except:
                        continue
                if not date_filled:
                    logging.error("Could not fill dates")
                    continue
                await page.wait_for_timeout(1000)

                # Click search
                search_clicked = False
                search_selectors = [
                    'button:has-text("Search")',
                    'button[type="submit"]',
                    'button[class*="search" i]',
                    'input[type="submit"]'
                ]
                for selector in search_selectors:
                    try:
                        search_button = page.locator(selector).first
                        await search_button.click()
                        search_clicked = True
                        break
                    except:
                        continue
                if not search_clicked:
                    logging.error("Could not click search button")
                    continue
                await page.wait_for_load_state('networkidle', timeout=30000)
                await page.wait_for_timeout(5000)

                # Verify we're on search page
                if "/search" not in page.url:
                    logging.warning(f"Not on search page. URL: {page.url}")
                    continue

                # Sort by highest miles - FIXED VERSION
                current_url = page.url
                if "sort=" not in current_url:
                    # The correct parameter is "milesHighest" not "miles_desc"
                    sorted_url = current_url + ("&sort=milesHighest" if "?" in current_url else "?sort=milesHighest")
                    await page.goto(sorted_url, wait_until='networkidle', timeout=30000)
                    await page.wait_for_timeout(3000)
                    logging.info("Sorted by highest miles")
                else:
                    logging.info("Results already sorted")

                # Extract hotels - FIXED VERSION
                hotels = await page.evaluate('''() => {
                    const results = [];
                    const cards = document.querySelectorAll('[data-testid*="hotel"], [class*="hotel-card"], article, div[class*="property"]');
                    
                    console.log('Found ' + cards.length + ' hotel cards');
                    
                    for (let i = 0; i < Math.min(cards.length, 30); i++) {
                        const card = cards[i];
                        const text = card.textContent || '';
                        
                        // Look for "Earn 10,000 miles" OR "Earn 10000 miles" with any text after
                        // This will match "Earn 10,000 miles per stay" etc.
                        if ((text.includes('Earn 10,000 miles') || text.includes('Earn 10000 miles')) && 
                            !text.includes('Earn 1,000 miles') && 
                            !text.includes('Earn 1000 miles')) {
                            
                            // Find hotel name - look for h3, h2, or elements with hotel name classes
                            let name = '';
                            const nameSelectors = [
                                '[data-testid="hotel-name"]',
                                'h3',
                                'h2',
                                '[class*="hotel-name"]',
                                '[class*="property-name"]',
                                'a[href*="/hotel/"]'
                            ];
                            
                            for (const sel of nameSelectors) {
                                const nameEl = card.querySelector(sel);
                                if (nameEl && nameEl.textContent) {
                                    const tempName = nameEl.textContent.trim();
                                    // Make sure it's actually a hotel name, not other text
                                    if (tempName && tempName.length > 3 && !tempName.includes('$') && !tempName.includes('miles')) {
                                        name = tempName;
                                        break;
                                    }
                                }
                            }
                            
                            // Find price - look for dollar amounts
                            let price = null;
                            // Match prices like $389, $1,234, etc.
                            const priceMatches = text.match(/\\$(\d{1,3}(?:,\d{3})*(?:\\.\\d{2})?)/g);
                            if (priceMatches && priceMatches.length > 0) {
                                // Usually the last price is the total
                                const lastPrice = priceMatches[priceMatches.length - 1];
                                price = parseFloat(lastPrice.replace('$', '').replace(/,/g, ''));
                            }
                            
                            if (name && price) {
                                console.log('Found 10K hotel: ' + name + ' at $' + price);
                                results.push({
                                    name: name,
                                    price: price,
                                    points: 10000
                                });
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
                logging.info(f"Found {len(hotels)} hotels with 10K points")
            except Exception as e:
                logging.error(f"Error scraping {city} for {checkin_date}: {str(e)}")
                await page.screenshot(path=f"error_{city.replace(',', '_').replace(' ', '_')}_{date_index}.png")
    finally:
        await page.close()
    return all_results

async def process_batch(start_index, batch_size):
    """Process a batch of cities"""
    # Load cities
    cities_file = os.getenv('CITIES_FILE', 'cities_top200.txt')
    if os.path.exists(cities_file):
        with open(cities_file, 'r') as f:
            all_cities = [line.strip() for line in f if line.strip()]
    else:
        all_cities = ["Bangkok, Thailand", "Istanbul, Turkey", "London, UK", "Dubai, UAE", "Singapore"]
    # Get batch
    cities = all_cities[start_index:start_index + batch_size]
    logging.info(f"Processing batch: {len(cities)} cities starting from index {start_index}")
    # Generate dates - today, tomorrow, day after tomorrow
    dates_to_check = []
    for days_ahead in [0, 1, 2]:
        checkin = datetime.now() + timedelta(days=days_ahead)
        checkout = checkin + timedelta(days=1)
        dates_to_check.append((
            checkin.strftime("%m/%d/%Y"),
            checkout.strftime("%m/%d/%Y")
        ))
    all_results = []
    async with async_playwright() as p:
        browser = await create_browser(p)
        context = await create_context(browser)
        try:
            for i, city in enumerate(cities):
                logging.info(f"\n{'='*60}")
                logging.info(f"City {i+1}/{len(cities)}: {city}")
                logging.info(f"{'='*60}")
                city_results = await scrape_city(context, city, dates_to_check, i)
                if city_results:
                    # Find cheapest
                    cheapest = min(city_results, key=lambda x: x['Price'])
                    all_results.append(cheapest)
                    logging.info(f"✅ Cheapest: {cheapest['Hotel']} - ${cheapest['Price']:.2f}")
                else:
                    logging.warning(f"❌ No 10K hotels found in {city}")
                # Delay between cities
                if i < len(cities) - 1:
                    delay = random.uniform(10, 20)
                    logging.info(f"Waiting {delay:.1f}s before next city...")
                    await asyncio.sleep(delay)
        finally:
            await context.close()
            await browser.close()
    return all_results

async def main():
    # Get batch parameters from environment
    batch_start = int(os.getenv('BATCH_START', 0))
    batch_size = int(os.getenv('BATCH_SIZE', 10))
    # Process batch
    results = await process_batch(batch_start, batch_size)
    # Save results
    if results:
        # Save batch results
        batch_file = f"cheapest_10k_hotels_batch_{batch_start}.csv"
        df = pd.DataFrame(results)
        df.to_csv(batch_file, index=False)
        logging.info(f"Saved {len(results)} results to {batch_file}")
        # Also save/update master file
        master_file = "cheapest_10k_hotels_all.csv"
        if os.path.exists(master_file):
            existing_df = pd.read_csv(master_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['City'], keep='last')
            combined_df.to_csv(master_file, index=False)
        else:
            df.to_csv(master_file, index=False)
        # Print summary
        print(f"\n{'='*60}")
        print(f"BATCH {batch_start} RESULTS:")
        print(f"{'='*60}")
        for r in sorted(results, key=lambda x: x['Price']):
            print(f"{r['City']}: ${r['Price']:.2f} - {r['Hotel']}")
    else:
        logging.warning("No results found in this batch")

if __name__ == "__main__":
    asyncio.run(main())

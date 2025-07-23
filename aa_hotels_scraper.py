import argparse
import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import random
import os
import re

# ======================
# CLI ARGUMENTS
# ======================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cities', type=str, default=None)
    parser.add_argument('--proxies', type=str, default=None)
    parser.add_argument('--concurrent', type=int, default=3)
    parser.add_argument('--batch-start', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=10)
    return parser.parse_args()

args = parse_args()

# ======================
# LOGGING SETUP
# ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ======================
# BROWSER SETUP
# ======================
async def create_browser(playwright):
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
    browser = await playwright.chromium.launch(headless=True, args=browser_args)
    return browser

async def create_context(browser):
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
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}, app: {}};
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'permissions', {get: () => ['geolocation','notifications']});
    """)
    await context.route('**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,eot}', lambda route: route.abort())
    await context.route('**/analytics/**', lambda route: route.abort())
    await context.route('**/google-analytics.com/**', lambda route: route.abort())
    await context.route('**/googletagmanager.com/**', lambda route: route.abort())
    return context

async def handle_cloudflare(page):
    try:
        title = await page.title()
        if "just a moment" in title.lower():
            logging.info("Cloudflare challenge detected, waiting...")
            for _ in range(10):
                await page.wait_for_timeout(2000)
                title = await page.title()
                if "just a moment" not in title.lower() and "aadvantage" in title.lower():
                    logging.info("Cloudflare challenge passed!")
                    return True
            return False
        return True
    except Exception:
        return True

# ======================
# MAIN SCRAPE LOGIC
# ======================
async def scrape_city(context, city, dates_to_check, city_index):
    all_results = []
    page = await context.new_page()
    try:
        for date_index, (checkin_date, checkout_date) in enumerate(dates_to_check):
            logging.info(f"Checking {city} for {checkin_date} to {checkout_date}")
            try:
                await page.goto("https://www.aadvantagehotels.com/", wait_until='domcontentloaded', timeout=60000)
                await page.wait_for_timeout(3000)
                if not await handle_cloudflare(page):
                    logging.error("Failed to bypass Cloudflare")
                    continue
                await page.wait_for_timeout(2000)
                # ... rest of your scrape logic remains unchanged ...
                # (full scrape_city body is unchanged; omitted here for brevity)
                # You can paste your original scrape_city code here!

            except Exception as e:
                logging.error(f"Error scraping {city} for {checkin_date}: {str(e)}")
                await page.screenshot(path=f"error_{city.replace(',', '_').replace(' ', '_')}_{date_index}.png")
    finally:
        await page.close()
    return all_results

async def process_batch(start_index, batch_size):
    # --- LOAD THE CITIES FILE PASSED VIA --cities ARGUMENT! ---
    cities_file = args.cities or os.getenv('CITIES_FILE', 'cities_top200.txt')
    print(f"\n🔎 Loading cities from: {cities_file}\n")
    if os.path.exists(cities_file):
        with open(cities_file, 'r') as f:
            cities = [line.strip() for line in f if line.strip()]
    else:
        cities = ["Bangkok, Thailand", "Istanbul, Turkey", "London, UK", "Dubai, UAE", "Singapore"]
    logging.info(f"Processing batch: {len(cities)} cities from file {cities_file}")

    # -- DATES TO CHECK --
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
                    cheapest = min(city_results, key=lambda x: x['Price'])
                    all_results.append(cheapest)
                    logging.info(f"✅ Cheapest: {cheapest['Hotel']} - ${cheapest['Price']:.2f} for {cheapest['Check-in']} to {cheapest['Check-out']}")
                else:
                    logging.warning(f"❌ No 10K hotels found in {city}")
                if i < len(cities) - 1:
                    delay = random.uniform(10, 20)
                    logging.info(f"Waiting {delay:.1f}s before next city...")
                    await asyncio.sleep(delay)
        finally:
            await context.close()
            await browser.close()
    return all_results

async def main():
    batch_start = args.batch_start
    batch_size = args.batch_size

    print(f"\n{'='*80}")
    print(f"AAdvantage Hotel Scraper - Starting Run")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")
    print(f"Batch: {batch_start} to {batch_start + batch_size - 1}")
    print(f"{'='*80}\n")

    results = await process_batch(batch_start, batch_size)

    if results:
        batch_file = f"cheapest_10k_hotels_batch_{batch_start}.csv"
        df = pd.DataFrame(results)
        df.to_csv(batch_file, index=False)
        logging.info(f"Saved {len(results)} results to {batch_file}")

        master_file = "cheapest_10k_hotels_all.csv"
        if os.path.exists(master_file):
            existing_df = pd.read_csv(master_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['City'], keep='last')
            combined_df.to_csv(master_file, index=False)
        else:
            df.to_csv(master_file, index=False)

        # ... (rest of your pretty-print summary output here) ...
        # You can paste your existing pretty print and summary logic as-is.
        # Omitted here for brevity.

    else:
        logging.warning("No results found in this batch")
        print(f"\n⚠️  No 10,000-point hotels found in batch {batch_start}")

    print(f"\n{'='*80}")
    print(f"Run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    asyncio.run(main())

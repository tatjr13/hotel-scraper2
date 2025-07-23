import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import random
import os
import math
import re

# ----- BATCH SPLITTING LOGIC -----
def ensure_batches_exist(cities_file, num_batches):
    # Only splits if batch_0 doesn't exist
    if not os.path.exists(f'cities_batch_0.txt'):
        print('Batch files not found. Creating them now...')
        with open(cities_file, 'r') as f:
            cities = [line.strip() for line in f if line.strip()]
        n = len(cities)
        per_batch = math.ceil(n / num_batches)
        for i in range(num_batches):
            batch_cities = cities[i*per_batch:(i+1)*per_batch]
            out_file = f'cities_batch_{i}.txt'
            with open(out_file, 'w') as f_out:
                for city in batch_cities:
                    f_out.write(city + '\n')
            print(f"Wrote batch {i}: {len(batch_cities)} cities -> {out_file}")
    else:
        print('Batch files already exist. Skipping split.')

def load_batch_cities(batch_num):
    batch_file = f'cities_batch_{batch_num}.txt'
    if not os.path.exists(batch_file):
        raise FileNotFoundError(f"Batch file {batch_file} does not exist!")
    with open(batch_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

# ----- SCRAPER LOGIC -----
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
                    except Exception:
                        continue
                if not city_filled:
                    logging.error(f"Could not find city input for {city}")
                    continue
                await page.wait_for_timeout(2000)

                # Handle dropdown
                try:
                    await page.wait_for_selector('ul[role="listbox"] li, .suggestion', timeout=5000)
                    await page.locator('ul[role="listbox"] li, .suggestion').first.click()
                except Exception:
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
                    except Exception:
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
                    except Exception:
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

                # Select "Earn miles" - handle overlay blocking issue
                try:
                    earn_miles_radio = page.locator('input[type="radio"][value="earn" i], input[type="radio"][id*="earn" i]')
                    if await earn_miles_radio.count() > 0:
                        is_checked = await earn_miles_radio.first.is_checked()
                        if not is_checked:
                            try:
                                await earn_miles_radio.first.click()
                                await page.wait_for_timeout(2000)
                                logging.info("Clicked 'Earn miles' radio option")
                            except:
                                earn_label = page.locator('label:has-text("Earn miles")')
                                if await earn_label.count() > 0:
                                    await earn_label.first.click()
                                    await page.wait_for_timeout(2000)
                                    logging.info("Clicked 'Earn miles' label instead")
                        else:
                            logging.info("'Earn miles' already selected")
                except Exception as e:
                    logging.warning(f"Could not select 'Earn miles' radio: {e}")

                # Sort by most miles - with better fallback
                sorted_successfully = False
                try:
                    sort_dropdown = page.locator('select[name="sort"], select[aria-label*="sort" i]').first
                    if await sort_dropdown.count() > 0:
                        current_value = await sort_dropdown.input_value()
                        if current_value != "milesHighest":
                            await sort_dropdown.click()
                            await page.wait_for_timeout(500)
                            await sort_dropdown.select_option(value="milesHighest")
                            await page.wait_for_timeout(3000)
                            logging.info("Selected 'Most miles earned' from dropdown")
                        else:
                            logging.info("Already sorted by 'Most miles earned'")
                        sorted_successfully = True
                except Exception as e:
                    logging.warning(f"Could not set sort dropdown: {e}")

                if not sorted_successfully:
                    try:
                        current_url = page.url
                        if "sort=milesHighest" not in current_url:
                            cleaned_url = re.sub(r'[?&]sort=[^&]*', '', current_url)
                            separator = '&' if '?' in cleaned_url else '?'
                            sorted_url = f"{cleaned_url}{separator}sort=milesHighest"
                            await page.goto(sorted_url, wait_until='networkidle', timeout=30000)
                            await page.wait_for_timeout(3000)
                            logging.info("Sorted by URL parameter")
                    except Exception as e:
                        logging.error(f"Failed to sort via URL: {e}")

                await page.screenshot(path=f"debug_{city.replace(',', '_').replace(' ', '_')}_{date_index}_sorted.png")

                hotels = await page.evaluate('''() => {
                    const results = [];
                    const cards = document.querySelectorAll('[data-testid*="hotel"], [class*="hotel-card"], article, div[class*="property"]');
                    for (let i = 0; i < Math.min(cards.length, 30); i++) {
                        const card = cards[i];
                        const text = card.textContent || '';
                        if ((text.includes('Earn 10,000 miles') || text.includes('Earn 10000 miles')) &&
                            !text.includes('Earn 1,000 miles') && !text.includes('Earn 1000 miles')) {
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
                                    if (tempName && tempName.length > 3 && !tempName.includes('$')) {
                                        name = tempName;
                                        break;
                                    }
                                }
                            }
                            let price = null;
                            const pricePatterns = [
                                /Total[^$]*\\$(\d{1,4}(?:,\\d{3})*(?:\\.\\d{2})?)/i,
                                /\\$(\d{1,4}(?:,\\d{3})*(?:\\.\\d{2})?)\\s*(?:Total|per|\\/)/i,
                                /\\$(\d{1,4}(?:,\\d{3})*(?:\\.\\d{2})?)/
                            ];
                            for (const pattern of pricePatterns) {
                                const match = text.match(pattern);
                                if (match) {
                                    const extractedPrice = parseFloat(match[1].replace(/,/g, ''));
                                    if (extractedPrice >= 20 && extractedPrice <= 5000) {
                                        price = extractedPrice;
                                        break;
                                    }
                                }
                            }
                            if (name && price) {
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

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # --- SET THESE VALUES ---
    CITIES_FILE = 'cities_top200.txt'
    NUM_BATCHES = int(os.getenv('NUM_BATCHES', '5'))  # Or hardcode as needed
    batch_num = int(os.getenv('BATCH_NUM', '0'))
    # --- BATCH SPLIT LOGIC ---
    ensure_batches_exist(CITIES_FILE, NUM_BATCHES)
    cities = load_batch_cities(batch_num)

    print(f"\n==== Starting batch {batch_num}: {len(cities)} cities ====")

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

    if all_results:
        batch_file = f"cheapest_10k_hotels_batch_{batch_num}.csv"
        df = pd.DataFrame(all_results)
        df.to_csv(batch_file, index=False)
        print(f"Saved batch {batch_num} to {batch_file}")
    else:
        print(f"No results found for batch {batch_num}")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import random
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
                        # Check if it's already selected
                        is_checked = await earn_miles_radio.first.is_checked()
                        if not is_checked:
                            # Try to click the radio button
                            try:
                                await earn_miles_radio.first.click()
                                await page.wait_for_timeout(2000)
                                logging.info("Clicked 'Earn miles' radio option")
                            except:
                                # If direct click fails, try the label
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
                        # Get current value
                        current_value = await sort_dropdown.input_value()
                        if current_value != "milesHighest":
                            # Force the dropdown to update
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

                # If dropdown didn't work, try URL parameter
                if not sorted_successfully:
                    try:
                        current_url = page.url
                        if "sort=milesHighest" not in current_url:
                            # Remove any existing sort parameter
                            cleaned_url = re.sub(r'[?&]sort=[^&]*', '', current_url)
                            # Add proper separator
                            separator = '&' if '?' in cleaned_url else '?'
                            sorted_url = f"{cleaned_url}{separator}sort=milesHighest"
                            await page.goto(sorted_url, wait_until='networkidle', timeout=30000)
                            await page.wait_for_timeout(3000)
                            logging.info("Sorted by URL parameter")
                    except Exception as e:
                        logging.error(f"Failed to sort via URL: {e}")

                # Take screenshot to verify sort worked
                await page.screenshot(path=f"debug_{city.replace(',', '_').replace(' ', '_')}_{date_index}_sorted.png")

                # Extract hotels with improved price extraction
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
                            
                            // Improved price extraction
                            let price = null;
                            // Look for price patterns like "$322" or "322 USD" or "Total (1 night) $322"
                            const pricePatterns = [
                                /Total[^$]*\\$(\d{1,4}(?:,\d{3})*(?:\\.\\d{2})?)/i,
                                /\\$(\d{1,4}(?:,\d{3})*(?:\\.\\d{2})?)\\s*(?:Total|per|\\/)/i,
                                /\\$(\d{1,4}(?:,\d{3})*(?:\\.\\d{2})?)/
                            ];
                            
                            for (const pattern of pricePatterns) {
                                const match = text.match(pattern);
                                if (match) {
                                    const extractedPrice = parseFloat(match[1].replace(/,/g, ''));
                                    // Sanity check - hotel prices should be between $20 and $5000
                                    if (extractedPrice >= 20 && extractedPrice <= 5000) {
                                        price = extractedPrice;
                                        break;
                                    }
                                }
                            }
                            
                            if (name && price) {
                                console.log('Found 10K hotel:', name, 'at $', price);
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

async def process_batch(start_index, batch_size):
    cities_file = os.getenv('CITIES_FILE', 'cities_top200.txt')
    if os.path.exists(cities_file):
        with open(cities_file, 'r') as f:
            all_cities = [line.strip() for line in f if line.strip()]
    else:
        all_cities = ["Bangkok, Thailand", "Istanbul, Turkey", "London, UK", "Dubai, UAE", "Singapore"]
    cities = all_cities[start_index:start_index + batch_size]
    logging.info(f"Processing batch: {len(cities)} cities starting from index {start_index}")
    logging.info(f"First city in this batch: {cities[0] if cities else 'None'}")
    logging.info(f"Last city in this batch: {cities[-1] if cities else 'None'}")
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
    # FIXED: Use BATCH_NUM from environment variable and calculate the actual start index
    batch_num = int(os.getenv('BATCH_NUM', 0))
    batch_size = int(os.getenv('BATCH_SIZE', 10))
    batch_start = batch_num * batch_size  # THIS IS THE KEY FIX!
    
    print(f"\n{'='*80}")
    print(f"AAdvantage Hotel Scraper - Starting Run")
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")
    print(f"Batch Number: {batch_num}")
    print(f"Batch Start Index: {batch_start}")
    print(f"Batch Size: {batch_size}")
    print(f"Processing cities {batch_start} to {batch_start + batch_size - 1}")
    print(f"{'='*80}\n")
    
    # Now process_batch doesn't need any parameters
    results = await process_batch()
    
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
        
        # Enhanced results display
        print(f"\n{'='*80}")
        print(f"BATCH {batch_start} COMPLETED - SUMMARY REPORT")
        print(f"{'='*80}")
        print(f"Total cities processed: {len(results)}")
        print(f"Average price for 10K points: ${df['Price'].mean():.2f}")
        print(f"Best value: ${df['Price'].min():.2f} ({df.loc[df['Price'].idxmin(), 'City']})")
        print(f"{'='*80}\n")
        
        # Sort results by price
        sorted_results = sorted(results, key=lambda x: x['Price'])
        
        # Prepare data for tabular display
        table_data = []
        for r in sorted_results:
            table_data.append([
                r['City'],
                r['Hotel'][:40] + '...' if len(r['Hotel']) > 40 else r['Hotel'],
                f"${r['Price']:.2f}",
                r['Check-in'],
                f"${r['Cost per Point']:.4f}"
            ])
        
        # Display results in a nice table (custom implementation)
        headers = ['City', 'Hotel', 'Price', 'Check-in', 'CPP']
        
        # Calculate column widths
        col_widths = [len(h) for h in headers]
        for row in table_data:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))
        
        # Add some padding
        col_widths = [w + 2 for w in col_widths]
        
        # Print header
        print("+" + "+".join(["-" * w for w in col_widths]) + "+")
        print("|" + "|".join([f" {headers[i]:<{col_widths[i]-2}} " for i in range(len(headers))]) + "|")
        print("+" + "+".join(["-" * w for w in col_widths]) + "+")
        
        # Print rows
        for row in table_data:
            print("|" + "|".join([f" {str(row[i]):<{col_widths[i]-2}} " for i in range(len(row))]) + "|")
        print("+" + "+".join(["-" * w for w in col_widths]) + "+")
        
        # Top 5 best deals
        print(f"\n🏆 TOP 5 BEST DEALS (Lowest Price for 10K Points):")
        print(f"{'='*80}")
        for i, r in enumerate(sorted_results[:5], 1):
            print(f"{i}. {r['City']}")
            print(f"   Hotel: {r['Hotel']}")
            print(f"   Price: ${r['Price']:.2f} ({r['Check-in']} to {r['Check-out']})")
            print(f"   Cost per point: ${r['Cost per Point']:.4f}")
            print()
        
        # Save summary report
        summary_file = f"batch_{batch_start}_summary.txt"
        with open(summary_file, 'w') as f:
            f.write(f"AAdvantage Hotel Scraper - Batch {batch_start} Summary\n")
            f.write(f"Run completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}\n")
            f.write(f"{'='*60}\n\n")
            f.write(f"Total cities processed: {len(results)}\n")
            f.write(f"Average price: ${df['Price'].mean():.2f}\n")
            f.write(f"Best value: ${df['Price'].min():.2f} ({df.loc[df['Price'].idxmin(), 'City']})\n\n")
            f.write("All results (sorted by price):\n")
            f.write(f"{'='*60}\n")
            for r in sorted_results:
                f.write(f"{r['City']}: ${r['Price']:.2f} - {r['Hotel']} ({r['Check-in']})\n")
        
        print(f"\n📄 Summary saved to: {summary_file}")
        print(f"📊 Full data saved to: {batch_file}")
        print(f"📁 Master file updated: {master_file}")
        
    else:
        logging.warning("No results found in this batch")
        print(f"\n⚠️  No 10,000-point hotels found in batch {batch_start}")
        
        # CRITICAL: Create empty CSV file even when no results
        batch_file = f"cheapest_10k_hotels_batch_{batch_start}.csv"
        empty_df = pd.DataFrame(columns=['City', 'Hotel', 'Price', 'Points', 'Check-in', 'Check-out', 'Cost per Point'])
        empty_df.to_csv(batch_file, index=False)
        logging.info(f"Created empty results file: {batch_file}")
    
    print(f"\n{'='*80}")
    print(f"Run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    asyncio.run(main())

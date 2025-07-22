# aa_hotels_scraper.py

import argparse
import random
import pandas as pd
import time

def load_proxies(proxy_file):
    with open(proxy_file) as f:
        # Expects http://user:pass@host:port per line
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

def get_cities(filename, start, size):
    with open(filename) as f:
        cities = [line.strip() for line in f if line.strip()]
    return cities[start:start+size]

def fake_hotel_search(city, proxy):
    # Replace with your real Playwright scraping code!
    # For now, we'll fake a small random delay
    time.sleep(random.uniform(0.2, 0.6))
    return {
        "City": city,
        "Hotel": f"Hotel in {city}",
        "Price": random.randint(50, 200),
        "Cost per Point": round(random.uniform(0.005, 0.012), 4),
        "ProxyUsed": proxy,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch-start', type=int, default=0)
    parser.add_argument('--batch-size', type=int, default=2)
    parser.add_argument('--proxies', type=str, default='formatted_proxies.txt')
    parser.add_argument('--cities', type=str, default='cities_top200.txt')
    args = parser.parse_args()

    proxies = load_proxies(args.proxies)
    cities = get_cities(args.cities, args.batch_start, args.batch_size)
    print(f"Processing {len(cities)} cities, rotating through {len(proxies)} proxies.")

    results = []
    for idx, city in enumerate(cities):
        # Cycle through proxies
        proxy = proxies[idx % len(proxies)]
        try:
            result = fake_hotel_search(city, proxy)
            results.append(result)
            print(f"[OK] {city} - {result['Hotel']} (${result['Price']}) [{proxy}]")
        except Exception as e:
            print(f"[FAIL] {city} - Proxy {proxy} failed: {e}")
            continue

    # Save results
    df = pd.DataFrame(results)
    df.to_csv('cheapest_10k_hotels_by_city.csv', index=False)
    print("Saved results to cheapest_10k_hotels_by_city.csv")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Minimal working version of AA Hotels Scraper
This version generates fake data to test the workflow
"""
import argparse
import pandas as pd
import random
import time
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="AA Hotels Scraper (Minimal Test Version)")
    parser.add_argument("--cities", default="cities_top200.txt", help="Cities file")
    parser.add_argument("--proxies", default="formatted_proxies.txt", help="Proxies file")
    parser.add_argument("--batch-start", type=int, default=0, help="Batch start index")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size")
    parser.add_argument("--concurrent", type=int, default=5, help="Max concurrent workers")
    parser.add_argument("--no-proxy", action="store_true", help="Run without proxies")
    
    args = parser.parse_args()
    
    logging.info(f"Starting scraper with batch-start={args.batch_start}, batch-size={args.batch_size}")
    
    # Load cities
    cities = []
    if os.path.exists(args.cities):
        with open(args.cities, 'r') as f:
            all_cities = [line.strip() for line in f if line.strip()]
            cities = all_cities[args.batch_start:args.batch_start + args.batch_size]
    else:
        logging.error(f"Cities file not found: {args.cities}")
        cities = [f"TestCity{i}" for i in range(args.batch_start, args.batch_start + args.batch_size)]
    
    logging.info(f"Processing {len(cities)} cities")
    
    # Generate fake results
    results = []
    for city in cities:
        # Simulate processing time
        time.sleep(random.uniform(0.1, 0.3))
        
        # 70% chance of finding a hotel
        if random.random() < 0.7:
            result = {
                'City': city,
                'Hotel': f"Test Hotel in {city}",
                'Price': round(random.uniform(50, 200), 2),
                'Points': 10000,
                'Check-in': '2024-01-15',
                'Check-out': '2024-01-16',
                'Cost per Point': round(random.uniform(0.005, 0.02), 4)
            }
            results.append(result)
            logging.info(f"✅ Found hotel in {city}: {result['Hotel']} - ${result['Price']}")
        else:
            logging.info(f"❌ No 10K hotels found in {city}")
    
    # Save results
    output_file = "cheapest_10k_hotels_by_city.csv"
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)
        logging.info(f"Saved {len(results)} results to {output_file}")
    else:
        # Create empty file with headers
        df = pd.DataFrame(columns=['City', 'Hotel', 'Price', 'Points', 'Check-in', 'Check-out', 'Cost per Point'])
        df.to_csv(output_file, index=False)
        logging.info("No results found, created empty results file")
    
    logging.info("Scraping complete!")

if __name__ == "__main__":
    main()

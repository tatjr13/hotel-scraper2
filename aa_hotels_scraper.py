import sys
import pandas as pd

def main():
    # Dummy results for demonstration
    cities = []
    with open("cities_top200.txt", "r") as f:
        for line in f:
            city = line.strip()
            if city:
                cities.append(city)
    hotels = [{"City": c, "Hotel": f"{c} Hotel", "Price": 100, "Cost per Point": 0.008} for c in cities]
    df = pd.DataFrame(hotels)
    df.to_csv("cheapest_10k_hotels_by_city.csv", index=False)
    print("Results saved to cheapest_10k_hotels_by_city.csv")

if __name__ == "__main__":
    main()

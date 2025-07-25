import os
import glob
import pandas as pd

def combine_batch_results():
    """
    Finds all batch result CSVs, combines them into a single DataFrame,
    and saves the output.
    """
    # Path where artifacts are downloaded
    search_path = "batch-results"
    
    # Use glob to find all batch CSV files downloaded from artifacts
    csv_pattern = os.path.join(search_path, "*", "hotels_batch_*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print("No batch result CSVs found to combine. Creating an empty results file.")
        # Create an empty DataFrame with the expected columns
        empty_df = pd.DataFrame(columns=['City', 'Hotel', 'Price', 'Points', 'Check-in', 'Check-out', 'Cost per Point'])
        empty_df.to_csv('hotels_all_results.csv', index=False)
        return

    print(f"Found {len(csv_files)} batch files to combine.")

    all_dfs = []
    for f in csv_files:
        try:
            # Read each CSV, ignoring empty or problematic files
            df = pd.read_csv(f)
            if not df.empty:
                all_dfs.append(df)
            else:
                print(f"Skipping empty file: {f}")
        except Exception as e:
            print(f"Error reading {f}, skipping. Reason: {e}")

    if not all_dfs:
        print("All found CSV files were empty or invalid. No combined file will be created.")
        return

    # Concatenate all DataFrames into one
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Sort results for consistency
    if 'City' in combined_df.columns:
        combined_df = combined_df.sort_values(by='City').reset_index(drop=True)

    # Save the final combined file
    output_filename = 'hotels_all_results.csv'
    combined_df.to_csv(output_filename, index=False)
    
    print(f"\n✅ Successfully combined {len(all_dfs)} files into {output_filename}.")
    print(f"Total rows in final file: {len(combined_df)}")

if __name__ == "__main__":
    combine_batch_results()

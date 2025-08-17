import pandas as pd
import os
import glob
from pathlib import Path


def clean_column_names(df):
    """Clean column names by removing extra spaces and standardizing"""
    df.columns = df.columns.str.strip()
    return df


def standardize_date_format(df):
    """Convert date column to standard datetime format"""
    if 'SEANCE' in df.columns:
        df['SEANCE'] = pd.to_datetime(df['SEANCE'], format='%d/%m/%Y', errors='coerce')
    return df


def clean_numeric_columns(df):
    """Clean and convert numeric columns"""
    numeric_columns = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                       'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']

    for col in numeric_columns:
        if col in df.columns:
            # Remove any non-numeric characters except decimal points and minus signs
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def process_csv_file(file_path):
    """Process a single CSV file"""
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            print(f"Could not read {file_path} with any encoding")
            return None

        # Clean column names
        df = clean_column_names(df)

        # Filter out separator rows (rows with dashes)
        df = df[~df.iloc[:, 0].astype(str).str.contains('---', na=False)]

        # Remove empty rows
        df = df.dropna(how='all')

        # Standardize date format
        df = standardize_date_format(df)

        # Clean numeric columns
        df = clean_numeric_columns(df)

        # Add source file information
        df['SOURCE_FILE'] = os.path.basename(file_path)
        df['SOURCE_FOLDER'] = os.path.basename(os.path.dirname(file_path))

        return df

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None


def combine_csv_files(base_path='.', start_year=2008, end_year=2024, output_file='combined_indices.csv'):
    """
    Combine CSV files from yearly folders

    Parameters:
    base_path (str): Base directory containing the yearly folders
    start_year (int): Starting year (default: 2008)
    end_year (int): Ending year (default: 2024)
    output_file (str): Name of the output combined file
    """

    all_dataframes = []

    # Process each year folder
    for year in range(start_year, end_year + 1):
        folder_name = f"indice_{year}"
        folder_path = os.path.join(base_path, folder_name)

        if not os.path.exists(folder_path):
            print(f"Folder {folder_name} not found, skipping...")
            continue

        print(f"Processing folder: {folder_name}")

        # Find all CSV files in the folder
        csv_pattern = os.path.join(folder_path, "*.csv")
        csv_files = glob.glob(csv_pattern)

        if not csv_files:
            print(f"No CSV files found in {folder_name}")
            continue

        # Process each CSV file in the folder
        folder_dataframes = []
        for csv_file in csv_files:
            print(f"  Processing: {os.path.basename(csv_file)}")
            df = process_csv_file(csv_file)
            if df is not None and not df.empty:
                folder_dataframes.append(df)

        # Combine files from this folder
        if folder_dataframes:
            folder_combined = pd.concat(folder_dataframes, ignore_index=True)
            all_dataframes.append(folder_combined)
            print(f"  Combined {len(folder_dataframes)} files from {folder_name}")

    if not all_dataframes:
        print("No data found to combine!")
        return None

    # Combine all years
    print("\nCombining all years...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Sort by date and code
    if 'SEANCE' in combined_df.columns and 'CODE_INDICE' in combined_df.columns:
        combined_df = combined_df.sort_values(['SEANCE', 'CODE_INDICE'])

    # Reset index
    combined_df.reset_index(drop=True, inplace=True)

    # Save to CSV
    combined_df.to_csv(output_file, index=False, encoding='utf-8')

    print(f"\nCombined dataset saved as: {output_file}")
    print(f"Total records: {len(combined_df)}")
    print(f"Date range: {combined_df['SEANCE'].min()} to {combined_df['SEANCE'].max()}")
    print(f"Unique indices: {combined_df['CODE_INDICE'].nunique()}")

    # Display sample of the data
    print("\nSample of combined data:")
    print(combined_df.head(10))

    # Display data info
    print("\nDataset information:")
    print(combined_df.info())

    return combined_df


def main():
    """Main function to run the CSV combiner"""

    # Configuration
    BASE_PATH = "."  # Current directory, change if needed
    START_YEAR = 2008
    END_YEAR = 2024
    OUTPUT_FILE = "combined_tunisian_indices.csv"

    print("=== Tunisian Stock Market Indices CSV Combiner ===\n")
    print(f"Base path: {os.path.abspath(BASE_PATH)}")
    print(f"Year range: {START_YEAR} - {END_YEAR}")
    print(f"Output file: {OUTPUT_FILE}")
    print("=" * 50)

    # Run the combination process
    combined_data = combine_csv_files(
        base_path=BASE_PATH,
        start_year=START_YEAR,
        end_year=END_YEAR,
        output_file=OUTPUT_FILE
    )

    if combined_data is not None:
        print(f"\n✅ Successfully combined CSV files!")
        print(f"📊 Final dataset shape: {combined_data.shape}")
    else:
        print("\n❌ Failed to combine CSV files!")


if __name__ == "__main__":
    main()
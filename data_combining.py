import pandas as pd
import os
import glob


def clean_column_names(df):
    """Clean column names by stripping spaces"""
    df.columns = df.columns.str.strip()
    return df


def standardize_date_format(df):
    """Convert date column to standard datetime format, handling multiple formats"""
    if 'SEANCE' in df.columns:
        df['SEANCE'] = pd.to_datetime(
            df['SEANCE'].astype(str).str.strip(),
            errors='coerce',
            dayfirst=True,  # handles DD/MM/YYYY
            infer_datetime_format=True
        )
    return df


def clean_numeric_columns(df):
    """Clean numeric columns and add missing ones"""
    numeric_columns = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                       'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',', '.').str.strip(),
                errors='coerce'
            )
        else:
            df[col] = pd.NA

    return df


def process_csv_file(file_path):
    """Process a single CSV file"""
    try:
        # Try multiple encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None
        for enc in encodings:
            try:
                df = pd.read_csv(file_path)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            print(f"Could not read {file_path}")
            return None

        # Clean column names
        df = clean_column_names(df)

        # Strip whitespace from string columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Remove separator rows
        df = df[~df.iloc[:, 0].astype(str).str.contains('---', na=False)]

        # Drop empty rows
        df = df.dropna(how='all')

        # Standardize date column
        df = standardize_date_format(df)

        # Clean numeric columns
        df = clean_numeric_columns(df)

        # Add source info
        df['SOURCE_FILE'] = os.path.basename(file_path)
        df['SOURCE_FOLDER'] = os.path.basename(os.path.dirname(file_path))

        return df

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None


def combine_csv_files(base_path='.', start_year=2008, end_year=2024, output_file='combined_indices.csv'):
    """Combine CSV files across multiple yearly folders"""
    all_dataframes = []

    for year in range(start_year, end_year + 1):
        folder_name = f"indice_{year}"
        folder_path = os.path.join(base_path, folder_name)

        if not os.path.exists(folder_path):
            print(f"Folder {folder_name} not found, skipping...")
            continue

        print(f"Processing folder: {folder_name}")

        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        if not csv_files:
            print(f"No CSV files found in {folder_name}")
            continue

        folder_dataframes = []
        for csv_file in csv_files:
            print(f"  Processing: {os.path.basename(csv_file)}")
            df = process_csv_file(csv_file)
            if df is not None and not df.empty:
                folder_dataframes.append(df)

        if folder_dataframes:
            folder_combined = pd.concat(folder_dataframes, ignore_index=True)
            all_dataframes.append(folder_combined)
            print(f"  Combined {len(folder_dataframes)} files from {folder_name}")

    if not all_dataframes:
        print("No data found to combine!")
        return None

    # Combine all years
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Ensure consistent column order
    columns_order = [
        'SEANCE', 'CODE_INDICE', 'LIB_INDICE',
        'INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
        'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV',
        'SOURCE_FILE', 'SOURCE_FOLDER'
    ]
    for col in columns_order:
        if col not in combined_df.columns:
            combined_df[col] = pd.NA

    combined_df = combined_df[columns_order]

    # Remove every entry with a null value in 'SEANCE' column BEFORE saving
    combined_df = combined_df.dropna(subset=['SEANCE'])

    # Sort by date and code
    combined_df = combined_df.sort_values(['SEANCE', 'CODE_INDICE']).reset_index(drop=True)

    # Save to CSV
    combined_df.to_csv(output_file, index=False, encoding='utf-8')

    print(f"\nCombined dataset saved as: {output_file}")
    print(f"Total records: {len(combined_df)}")
    print(f"Date range: {combined_df['SEANCE'].min()} to {combined_df['SEANCE'].max()}")
    print(f"Unique indices: {combined_df['CODE_INDICE'].nunique()}")
    print("\nSample of combined data:")
    print(combined_df.head(10))
    return combined_df


def main():
    BASE_PATH = "."  # adjust to your folder path
    START_YEAR = 2008
    END_YEAR = 2024
    OUTPUT_FILE = "combined_tunisian_indices.csv"

    print("=== Tunisian Stock Market Indices CSV Combiner ===\n")
    combine_csv_files(BASE_PATH, START_YEAR, END_YEAR, OUTPUT_FILE)


if __name__ == "__main__":
    main()

import pandas as pd
from data_combining import standardize_date_format

# Define corrected column specifications (start, end positions)
colspecs = [
    (0, 11),    # SEANCE
    (11, 22),   # CODE_INDICE
    (22, 47),   # LIB_INDICE
    (47, 59),   # INDICE_JOUR
    (59, 73),   # INDICE_VEILLE
    (73, 89),   # VARIATION_VEILLE
    (89, 105),  # INDICE_PLUS_HAUT
    (105, 121), # INDICE_PLUS_BAS
    (121, 137), # INDICE_OUV
]

# Read the fixed-width file
df = pd.read_fwf('histo_indice_2021.txt', colspecs=colspecs, skiprows=2, engine='python')

# Add column names
df.columns = [
    'SEANCE',
    'CODE_INDICE',
    'LIB_INDICE',
    'INDICE_JOUR',
    'INDICE_VEILLE',
    'VARIATION_VEILLE',
    'INDICE_PLUS_HAUT',
    'INDICE_PLUS_BAS',
    'INDICE_OUV'
]

# Standardize the date format
df = standardize_date_format(df)

# Clean numeric columns
numeric_columns = [
    'INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
    'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV'
]

for col in numeric_columns:
    if col in df.columns:
        # Remove spaces, replace commas with dots if present, and convert to float
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.').str.strip(), errors='coerce')

# Save to CSV
df.to_csv('histo_indice_2021.csv', index=False, encoding='utf-8')

print(df.head())

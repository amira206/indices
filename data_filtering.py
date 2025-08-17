import pandas as pd

# Read the CSV file
df = pd.read_csv('combined_tunisian_indices.csv')

# Filter rows where LIB_INDICE equals 'TUNINDEX'
filtered_df = df[df['LIB_INDICE'] == 'TUNINDEX']

# Save the filtered data to a new CSV file
filtered_df.to_csv('tunindex_filtered.csv', index=False)

print(f"Filtered data saved to 'tunindex_filtered.csv'")
print(f"Original data: {len(df)} rows")
print(f"Filtered data: {len(filtered_df)} rows")

# Display the first few rows of filtered data
print("\nFirst few rows of filtered data:")
print(filtered_df.head())
import pandas as pd
from sdv.multi_table import MultiTableMetadata, HMASynthesizer

def generate_synthetic_data(parquet_files, primary_key, output_files, num_samples=100):
    """
    Reads multiple Parquet files related by a primary key, generates synthetic data using SDV,
    and saves the synthetic data to new Parquet files.

    Args:
        parquet_files (dict): Dictionary of table names and file paths for the input Parquet files.
        primary_key (str): The primary key that relates the tables.
        output_files (dict): Dictionary of table names and file paths for the output synthetic data.
        num_samples (int): Number of synthetic data samples to generate for each table.
    """
    try:
        # Load the Parquet files into a dictionary of DataFrames
        print("Loading Parquet files...")
        data = {table_name: pd.read_parquet(file_path) for table_name, file_path in parquet_files.items()}
        print(f"Loaded data for tables: {list(data.keys())}")

        # Create MultiTableMetadata
        metadata = MultiTableMetadata()

        # Add tables to metadata and specify primary key relationships
        for table_name, df in data.items():
            print(f"Detecting metadata for table: {table_name}")
            metadata.detect_table_from_dataframe(table_name, df)

        # Define primary key relationships
        for table_name in data.keys():
            metadata.tables[table_name].set_primary_key(primary_key)

        # Train the HMASynthesizer
        print("Training HMASynthesizer...")
        synthesizer = HMASynthesizer(metadata)
        synthesizer.fit(data)

        # Generate synthetic data
        print("Generating synthetic data...")
        synthetic_data = synthesizer.sample(num_samples)

        # Save synthetic data to Parquet files
        print("Saving synthetic data to files...")
        for table_name, df in synthetic_data.items():
            output_file = output_files[table_name]
            print(f"Saving synthetic data for table {table_name} to {output_file}")
            df.to_parquet(output_file)

        print("Synthetic data generation complete.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Input Parquet files
    parquet_files = {
        "table1": "table1.parquet",
        "table2": "table2.parquet",
        "table3": "table3.parquet"
    }

    # Output Parquet files for synthetic data
    output_files = {
        "table1": "synthetic_table1.parquet",
        "table2": "synthetic_table2.parquet",
        "table3": "synthetic_table3.parquet"
    }

    # Primary key
    primary_key = "partyId"

    # Number of synthetic samples to generate
    number_of_samples = 500

    generate_synthetic_data(parquet_files, primary_key, output_files, number_of_samples)

import pandas as pd
from sdv.metadata import SingleTableMetadata

def generate_metadata(parquet_file, metadata_file):
    """
    Generates metadata for a Parquet dataset and saves it as a JSON file.

    Args:
        parquet_file (str): Path to the input Parquet file.
        metadata_file (str): Path to save the generated metadata file.
    """
    try:
        # Load the Parquet file into a pandas DataFrame
        print(f"Loading Parquet file: {parquet_file}")
        df = pd.read_parquet(parquet_file)
        print(f"Dataset loaded. Shape: {df.shape}")

        # Create metadata object
        print("Generating metadata...")
        metadata = SingleTableMetadata()

        # Detect metadata from the dataframe
        metadata.detect_from_dataframe(df)

        # Save the metadata to a file
        print(f"Saving metadata to: {metadata_file}")
        metadata.save_to_json(metadata_file)
        print("Metadata saved successfully.")

        # Display the metadata summary
        print("\nMetadata summary:")
        print(metadata.to_dict())

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Path to the input Parquet file
    input_parquet_file = "input_data.parquet"

    # Path to save the metadata JSON file
    metadata_output_file = "dataset_metadata.json"

    generate_metadata(input_parquet_file, metadata_output_file)

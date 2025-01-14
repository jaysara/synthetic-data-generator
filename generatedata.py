import pandas as pd
from sdv.tabular import GaussianCopula

def generate_synthetic_data(parquet_file, output_file, num_samples=100):
    """
    Reads a Parquet file, generates synthetic data using SDV, and saves the synthetic data to a new Parquet file.

    Args:
        parquet_file (str): Path to the input Parquet file.
        output_file (str): Path to save the synthetic data Parquet file.
        num_samples (int): Number of synthetic data samples to generate.
    """
    try:
        # Load the Parquet file into a pandas DataFrame
        print(f"Loading Parquet file: {parquet_file}")
        df = pd.read_parquet(parquet_file)
        print(f"Original data loaded. Shape: {df.shape}")

        # Train a GaussianCopula model
        print("Training SDV GaussianCopula model...")
        model = GaussianCopula()
        model.fit(df)

        # Generate synthetic data
        print(f"Generating {num_samples} synthetic data samples...")
        synthetic_data = model.sample(num_samples)
        print(f"Synthetic data generated. Shape: {synthetic_data.shape}")

        # Save the synthetic data to a Parquet file
        print(f"Saving synthetic data to: {output_file}")
        synthetic_data.to_parquet(output_file)
        print("Synthetic data saved successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Path to the input Parquet file
    input_parquet_file = "input_data.parquet"

    # Path to save the synthetic data Parquet file
    output_parquet_file = "synthetic_data.parquet"

    # Number of synthetic samples to generate
    number_of_samples = 500

    generate_synthetic_data(input_parquet_file, output_parquet_file, number_of_samples)

import requests
import pandas as pd
from io import BytesIO

def extract(parquet_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet", output_csv_path="/dbfs/yellow_tripdata_2024-01.csv"):
    response = requests.get(parquet_url, stream=True)
    if response.status_code == 200:
        parquet_data = BytesIO(response.content)
        print("Parquet file downloaded successfully.")
    else:
        print("Failed to download the Parquet file.")
        return

    # Read the Parquet file with Pandas
    df = pd.read_parquet(parquet_data)

    # Save DataFrame as CSV
    df.to_csv(output_csv_path, index=False)
    print(f"Data successfully saved as CSV at {output_csv_path}")

if __name__ == "__main__":
    extract()
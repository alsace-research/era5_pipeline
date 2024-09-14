import os
import logging
import time
import pandas as pd
import gcsfs
from dask.distributed import Client
from src.h3_processing import load_and_process_file, convert_timestamps_to_pandas, save_to_parquet
from src.utils import ensure_directory_exists, list_files_for_date_range

# Set up logging configuration
logging.basicConfig(
    filename='process_log.log',  # Log to a file (you can adjust the path)
    level=logging.INFO,  # Set log level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Date format
)

def run_pipeline(config):
    # Start the pipeline and log it
    logging.info("Pipeline started.")
    
    # Add this line to capture the start time of the pipeline
    start_time = time.time()

    # Ensure the output directory exists
    ensure_directory_exists(config['data']['output_path'])

    # GCS setup
    fs = gcsfs.GCSFileSystem()

    # Dask client setup
    client = Client(n_workers=config['dask']['num_workers'], 
                    memory_limit=config['dask']['memory_limit'], 
                    local_directory=config['dask']['local_directory'],
                    dashboard_address=":8787")
    
    logging.info(f"Dask dashboard available at {client.dashboard_link}")

    base_path = config['data']['storage_path']
    var_name = 'total_precipitation'
    start_date = pd.Timestamp(config['processing']['start_date'])
    end_date = pd.Timestamp(config['processing']['end_date'])

    # Log file listing
    logging.info(f"Listing files from {start_date} to {end_date}.")
    file_paths = list_files_for_date_range(base_path, start_date, end_date, var_name)

    combined_df = pd.DataFrame()

    # Process each file and log processing time
    for file_path in file_paths:
        file_start_time = time.time()
        logging.info(f"Processing file: {file_path}")

        final_df = load_and_process_file(file_path, fs, client, 
                                         threshold=config['data']['threshold'], 
                                         resolution=config['data']['resolution'])
        logging.info(f"File processed: {file_path} with shape: {final_df.shape}. Time taken: {time.time() - file_start_time:.2f} seconds")

        combined_df = pd.concat([combined_df, final_df], ignore_index=True)

    # Convert timestamps and ensure proper format for Parquet
    combined_df = convert_timestamps_to_pandas(combined_df)

    # Log before saving to Parquet
    logging.info(f"Saving the combined DataFrame to Parquet.")
    parquet_start_time = time.time()

    # Use the save_to_parquet function to save the DataFrame, optimized for querying
    save_to_parquet(combined_df, f"{config['data']['output_path']}/combined_output.parquet")
    
    logging.info(f"Data saved to Parquet. Time taken: {time.time() - parquet_start_time:.2f} seconds")

    # Log total runtime
    total_time = time.time() - start_time
    logging.info(f"Pipeline completed successfully in {total_time:.2f} seconds.")

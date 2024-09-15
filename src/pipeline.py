import pandas as pd
import gcsfs
import logging
import time
from datetime import timedelta
from dask.distributed import Client
from src.h3_processing import process_and_save_by_period, filter_by_date_range, load_and_process_file
from src.utils import ensure_directory_exists, list_files_for_date_range

# Set up logging configuration
logging.basicConfig(
    filename='process_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def split_date_range(start_date, end_date, period_days):
    """Split the date range into chunks of `period_days`."""
    current_start = pd.Timestamp(start_date)
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=period_days - 1), end_date)
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)

def run_pipeline(config):
    """Run the pipeline incrementally, processing data in chunks of `period_days`."""
    logging.info("Pipeline started.")
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

    # Date range and processing configuration
    start_date = pd.Timestamp(config['processing']['start_date'])
    end_date = pd.Timestamp(config['processing']['end_date'])
    period_days = config.get('processing', {}).get('period_days', 4)  # Default to 4-day periods

    # Loop through each period (e.g., 4-day periods)
    for period_start, period_end in split_date_range(start_date, end_date, period_days):
        period_start_time = time.time()
        logging.info(f"Processing period from {period_start} to {period_end}")

        # List files for the current 4-day period
        file_paths = list_files_for_date_range(config['data']['storage_path'], period_start, period_end, 'total_precipitation')

        # Process and combine the data for this period
        combined_df = pd.DataFrame()
        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")
            final_df = load_and_process_file(file_path, fs, client)
            if not final_df.empty:
                combined_df = pd.concat([combined_df, final_df], ignore_index=True)
            logging.info(f"Finished processing file: {file_path} with shape {final_df.shape}")

        # Check if combined_df is empty
        if combined_df.empty:
            logging.warning(f"No data for period from {period_start} to {period_end}. Skipping.")
            continue

        # Process and save the data for this period
        process_and_save_by_period(combined_df, f"{config['data']['output_path']}/combined_output", period_start, period_days)

        logging.info(f"Finished processing period from {period_start} to {period_end}. Time taken: {time.time() - period_start_time:.2f} seconds")

    # Log total runtime
    total_time = time.time() - start_time
    logging.info(f"Pipeline completed successfully in {total_time:.2f} seconds.")

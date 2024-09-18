import pandas as pd
import logging
import time
from dask.distributed import Client
from src.h3_processing import load_and_process_day_of_files
from src.utils import ensure_directory_exists
import os

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
        current_end = min(current_start + pd.Timedelta(days=period_days - 1), end_date)
        yield current_start, current_end
        current_start = current_end + pd.Timedelta(days=1)

def run_pipeline(config):
    """Run the pipeline incrementally, processing data in chunks of `period_days`."""
    logging.info("Pipeline started.")
    start_time = time.time()

    # Ensure the output directory exists
    ensure_directory_exists(config['data']['output_path'])
    ensure_directory_exists(config['data']['output_dir'])  # Create the output directory for Parquet files

    # Dask client setup
    client = Client(n_workers=config['dask']['num_workers'], 
                    memory_limit=config['dask']['memory_limit'], 
                    local_directory=config['dask']['local_directory'],
                    dashboard_address=f":{config['dask']['dashboard_port']}")
    
    logging.info(f"Dask dashboard available at {client.dashboard_link}")

    # Date range and processing configuration
    start_date = pd.Timestamp(config['processing']['start_date'])
    end_date = pd.Timestamp(config['processing']['end_date'])
    period_days = config['processing']['period_days']

    # Loop through each period (e.g., daily or multiple days depending on the config)
    for period_start, period_end in split_date_range(start_date, end_date, period_days):
        period_start_time = time.time()
        logging.info(f"Processing period from {period_start} to {period_end}")

        # File pattern for the current day
        file_pattern = os.path.join(config['data']['storage_path'], 
                                    config['data']['file_pattern'].format(year=period_start.year, 
                                                                           month=period_start.month, 
                                                                           day=period_start.day))

        # Chunk size settings from config
        chunk_size = config['processing']['chunk_size']

        # Load and process the day's files
        final_df = load_and_process_day_of_files(
            file_pattern, 
            client, 
            chunk_size, 
            config['data']['output_dir'],  # Corrected here
            threshold=config['data']['threshold'], 
            resolution=config['data']['resolution']
        )

        # Check if final_df is empty
        if final_df.empty:
            logging.warning(f"No data for period from {period_start} to {period_end}. Skipping.")
            continue

        logging.info(f"Finished processing period from {period_start} to {period_end}. Time taken: {time.time() - period_start_time:.2f} seconds")

    # Log total runtime
    total_time = time.time() - start_time
    logging.info(f"Pipeline completed successfully in {total_time:.2f} seconds.")

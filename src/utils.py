import os
import pandas as pd

def ensure_directory_exists(directory):
    """Ensure the specified directory exists, create it if not."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def list_files_for_date_range(base_path, start_date, end_date, var_name):
    """List all files in GCS for the specified date range."""
    file_paths = []
    for single_date in pd.date_range(start=start_date, end=end_date, freq='D'):
        year = single_date.year
        month = f'{single_date.month:02d}'
        day = f'{single_date.day:02d}'
        file_path = f"{base_path}/date-variable-single_level/{year}/{month}/{day}/{var_name}/surface.nc"
        file_paths.append(file_path)
    return file_paths

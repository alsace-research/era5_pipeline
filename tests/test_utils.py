import os
import pytest
from src.utils import ensure_directory_exists, list_files_for_date_range
import pandas as pd

def test_ensure_directory_exists(tmpdir):
    """Test if the directory is created when it does not exist."""
    test_dir = str(tmpdir.mkdir("test_dir"))
    
    # Remove the directory to test if it's recreated
    os.rmdir(test_dir)
    assert not os.path.exists(test_dir)
    
    # Call the function and ensure the directory exists
    ensure_directory_exists(test_dir)
    assert os.path.exists(test_dir)

def test_list_files_for_date_range():
    """Test if the correct file paths are generated for a given date range."""
    base_path = 'gs://bucket/path'
    start_date = pd.Timestamp('2022-01-01')
    end_date = pd.Timestamp('2022-01-02')
    var_name = 'total_precipitation'

    file_paths = list_files_for_date_range(base_path, start_date, end_date, var_name)
    
    expected_paths = [
        'gs://bucket/path/date-variable-single_level/2022/01/01/total_precipitation/surface.nc',
        'gs://bucket/path/date-variable-single_level/2022/01/02/total_precipitation/surface.nc'
    ]
    
    assert file_paths == expected_paths

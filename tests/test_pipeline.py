import pytest
import pandas as pd
from unittest.mock import patch, call
from src.pipeline import run_pipeline
import os

@pytest.fixture
def config():
    """Return a test configuration."""
    return {
        'data': {
            'output_path': './test_output',
            'output_dir': './test_output/parquet_files',  # Add output_dir here
            'storage_path': 'gs://test_bucket',
            'file_pattern': '{year}/{month:02d}/{day:02d}/total_precipitation/*.nc'  # Add the file_pattern key
        },
        'processing': {
            'start_date': '2022-01-01',
            'end_date': '2022-01-04',
            'period_days': 4
        },
        'dask': {
            'num_workers': 2,
            'memory_limit': '4GB',
            'local_directory': './test_dask_space',
            'dashboard_port': 8787  # Add the dashboard_port key here
        }
    }

@patch('src.utils.list_files_for_date_range')  # Adjust based on where list_files_for_date_range is defined
@patch('src.h3_processing.process_time_slice_futures')  # Example of mocking a processing function from h3_processing
@patch('src.pipeline.Client')
@patch('src.pipeline.os.makedirs')
def test_run_pipeline(mock_makedirs, mock_dask_client, mock_process_time_slice_futures, mock_list_files, config):
    """Test the full pipeline flow with mock objects."""
    # Mock file listing and processing results
    mock_list_files.return_value = ['gs://test_bucket/file1.parquet', 'gs://test_bucket/file2.parquet']
    mock_process_time_slice_futures.return_value = pd.DataFrame({
        'h3_index': [1, 2],
        'timestamp': pd.to_datetime(['2022-01-01', '2022-01-02']),
        'precipitation': [0.1, 0.2]
    })
    
    # Run the pipeline
    run_pipeline(config)
    
    # Assert that the directories were created for visuals
    mock_makedirs.assert_called_with('data/visuals', exist_ok=True)
    
    # Assert that the files were listed and processed
    mock_list_files.assert_called_once()
    mock_process_time_slice_futures.assert_called()

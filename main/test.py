import os
import zipfile
import requests
from typing import Tuple
from google.cloud import storage
from datetime import timedelta
from typing import List, Tuple
from prefect import flow, task
from prefect.tasks import task_input_hash, exponential_backoff


@task(name='download_data', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10), cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def download_data(url: str, output_dir: str) -> str:
    """
    Downloads a zip file from the specified url and saves it to the specified
    output directory.

    Args:
        url: The url of the zip file to download.
        output_dir: The directory to save the downloaded file in.

    Returns:
        The path to the downloaded file.
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, os.path.basename(url))

    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)

    return file_path
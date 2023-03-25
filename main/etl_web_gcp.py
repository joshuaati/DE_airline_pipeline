import os
import zipfile
import requests
import pandas as pd
from typing import Tuple
from google.cloud import storage
from datetime import timedelta
from typing import Tuple
from prefect import flow, task
from prefect.tasks import task_input_hash, exponential_backoff


os.environ['GOOGLE_APPLICATION_CREDENTIALS']='.google/airlinepipeline.json'


# prevent timeout due to low network
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB


@task(name='download_data', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10), cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
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
    
    # Create the output directory if it doesn't already exist
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the file path to save the downloaded file to
    file_path = os.path.join(output_dir, os.path.basename(url))

    # Download the file and save it to the specified path
    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)

    # Return the file path
    return file_path


@task(name='extract_data', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=5))
def extract_data(file_path: str, output_dir: str) -> Tuple[str, str]:
    """
    Extracts a zip file to the specified output directory.

    Args:
        file_path: The path to the zip file to extract.
        output_dir: The directory to extract the file to.

    Returns:
        A tuple containing the paths to the extracted files.
    """
    
    # Make sure the output directory exists, creating it if necessary
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract the zip file to the output directory
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    
    # Get a list of all the extracted files
    extracted_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            extracted_files.append(os.path.join(root, file))

    # Return the paths to the extracted files as a tuple
    return tuple(extracted_files)


@task(name='compress_files', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def compress_files(extracted_files: Tuple[str, ...], output_dir: str) -> Tuple[str, str]:
    """
    This function compresses extracted CSV files in .bz2 format into .gz format and saves them in the specified output
    directory. If the CSV file is encoded with 'latin_1', it will first be decoded before being compressed. Additionally,
    any occurrences of the characters '-', 'ä', 'æ', and 'â' in the 'TailNum' column will be stripped of whitespace.

    Parameters:
    extracted_files (Tuple[str, ...]): A tuple of file paths to CSV files in .bz2 format to be compressed.
    output_dir (str): The directory in which the compressed files will be saved.

    Returns:
    Tuple[str, str]: A tuple of file paths to the compressed files in the output directory.

    """
    for file_path in extracted_files:
        # Check if the file is in .bz2 format
        if file_path.endswith('.bz2'):
            compressed_file_path = file_path[:-4] + '.gz'
            try:
                # Try to read the file using the default encoding ('utf-8')
                df = pd.read_csv(file_path)
                # Compress the file and save it in the output directory
                df.to_csv(compressed_file_path, index=False, compression='gzip')
                # Remove the original file
                os.remove(file_path)
            except UnicodeDecodeError:
                # If the default encoding fails, try reading the file using 'latin_1' encoding
                df = pd.read_csv(file_path, encoding='latin_1')
                df['TailNum'] = df['TailNum'].str.strip('-äæâ')
                # Compress the file and save it in the output directory
                df.to_csv(compressed_file_path, index=False, compression='gzip')
                # Remove the original file
                os.remove(file_path)
    
    # Collect all file paths in the output directory
    all_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            all_files.append(os.path.join(root, file))
    
    # Return the file paths as a tuple
    return tuple(all_files)


@task(name='write_data', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def write_to_gcs(bucket_name: str, output_dir: str, extracted_files: Tuple[str, ...]):
    """
    Uploads the contents of each file in the extracted_files list to a Google Cloud Storage bucket.

    Args:
        bucket_name: The name of the Google Cloud Storage bucket to upload to.
        output_dir: The directory where the extracted files are stored.
        extracted_files: A tuple containing the paths to the extracted files.

    Returns:
        None
    """
    
    # Create a client for interacting with Google Cloud Storage
    storage_client = storage.Client()
    # Get the specified bucket
    bucket = storage_client.bucket(bucket_name)

    # Loop through each file to upload
    for file_path in extracted_files:
        # Read the contents of the file
        with open(file_path, 'rb') as f:
            content = f.read()

        # Construct the path to the file within the bucket
        filename = os.path.join(output_dir, os.path.basename(file_path))

        # Create a blob for the file and upload the contents
        blob = bucket.blob(filename)
        blob.upload_from_string(content)


@flow()
def main_etl(url: str, output_dir: str, bucket_name: str) -> None:
    """
    This function is the main entry point for the ETL (Extract, Transform, Load) pipeline that downloads a zip file from a specified URL,
    extracts it, and uploads the extracted files to a Google Cloud Storage bucket.

    Args:
        url : The URL of the zip file to download
        output_dir : The directory to save the downloaded and extracted files to.
        bucket_name : The name of the Google Cloud Storage bucket to upload the files to.

    Returns:
        None
    """
    file_path = download_data(url, output_dir)
    extracted_files = extract_data(file_path, output_dir)
    compressed_files = compress_files(extracted_files, output_dir)
    write_to_gcs(bucket_name, output_dir, compressed_files)


if __name__ == '__main__':
    url = "https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/HG7NV7"
    output_dir = "data"
    bucket_name = "airline-buckets"
    main_etl(url, output_dir, bucket_name)

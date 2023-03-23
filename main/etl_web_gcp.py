import os
import zipfile
import requests
import bz2
from typing import Tuple
from google.cloud import storage
from datetime import timedelta
from typing import List, Tuple
from prefect import flow, task
from prefect.tasks import task_input_hash, exponential_backoff


# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='Users/x/.google/credentials/airlinepipeline.json'
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
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, os.path.basename(url))

    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)

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
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    extracted_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            extracted_files.append(os.path.join(root, file))

    return tuple(extracted_files)


@task(name='extract_individual_files', retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=5))
def extract_individual_files(extracted_files: Tuple[str, ...]) -> Tuple[str, ...]:
    """
    Extracts individual files from any files with a .bz2 extension in a list of extracted files.

    Args:
        extracted_files: A tuple of file paths that have been extracted from a ZIP file.

    Returns:
        A tuple of file paths that includes any files with a .bz2 extension decompressed to individual files.

    """
    new_extracted_files = []
    for file_path in extracted_files:
        if file_path.endswith('.bz2'):
            with open(file_path, 'rb') as f:
                content = f.read()
            decompressed_content = bz2.decompress(content)
            new_file_path = file_path[:-4]  # remove .bz2 extension
            with open(new_file_path, 'wb') as f:
                f.write(decompressed_content)
            new_extracted_files.append(new_file_path)
        else:
            new_extracted_files.append(file_path)
    
    return tuple(new_extracted_files)

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
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file_path in extracted_files:
        with open(file_path, 'rb') as f:
            content = f.read()

        filename = os.path.join(output_dir, os.path.basename(file_path))

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
    individual_files = extract_individual_files(extracted_files)
    write_to_gcs(bucket_name, output_dir, individual_files)


if __name__ == '__main__':
    url = "https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/HG7NV7"
    output_dir = "data"
    bucket_name = "airline-buckets"
    main_etl(url, output_dir, bucket_name)

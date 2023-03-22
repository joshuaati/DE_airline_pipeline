import os
import zipfile
import requests
from typing import Tuple
from google.cloud import storage
from typing import List, Tuple
from prefect import flow, task
from prefect.tasks import task_input_hash


os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/Users/x/OneDrive/Documents/Python/Jan_2023/DE_Zoomcamp/week_4_analytics_engineering/.google/credentials/google_credentials.json'

# prevent timeout due to low network
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

@task(retries=3, )
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

    response = requests.get(url, stream=True)
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    return file_path


def extract_data(file_path: str, output_dir: str) -> Tuple[str, str]:
    """
    Extracts a zip file to the specified output directory.

    Args:
        file_path: The path to the zip file to extract.
        output_dir: The directory to extract the file to.

    Returns:
        A tuple containing the paths to the extracted files.
    """
    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    extracted_files = []
    for root, _, files in os.walk(output_dir):
        for file in files:
            extracted_files.append(os.path.join(root, file))

    return tuple(extracted_files)


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


url = 'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/GIZV7R'
output_dir = '../data'
bucket_name = 'my_bucket_name'

file_path = download_data(url, output_dir)
extracted_files = extract_data(file_path, output_dir)
print(extracted_files)
#write_to_gcs(bucket_name, output_dir, extracted_files)

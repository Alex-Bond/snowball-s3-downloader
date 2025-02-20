import csv
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple
import io

import boto3
import click
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError, ClientError
from tqdm import tqdm

def setup_logging(log_file: str = None):
    logger = logging.getLogger('snowball_copy')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file is specified)
    if log_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

class TqdmToLogger(io.StringIO):
    def __init__(self, logger, level=logging.INFO):
        super().__init__()
        self.logger = logger
        self.level = level

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)

def get_s3_file_list(s3: BaseClient, bucket_name: str) -> Tuple[Dict[str, int], int]:
    file_list = {}
    total_size = 0
    marker = None

    while True:
        if marker:
            response = s3.list_objects(Bucket=bucket_name, Marker=marker)
        else:
            response = s3.list_objects(Bucket=bucket_name)

        if 'Contents' in response:
            for obj in response['Contents']:
                file_list[obj['Key']] = obj['Size']
                total_size += obj['Size']

            if response.get('IsTruncated', False):
                marker = response['Contents'][-1]['Key']
            else:
                break
        else:
            break

    return file_list, total_size

def get_local_file_list(local_folder: str) -> Tuple[Dict[str, int], int]:
    file_list = {}
    total_size = 0
    for root, _, files in os.walk(local_folder):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, local_folder)
            size = os.path.getsize(file_path)
            file_list[relative_path] = size
            total_size += size
    return file_list, total_size

def download_file(s3: BaseClient, bucket_name: str, s3_key: str, local_path: str, logger: logging.Logger) -> Tuple[str | None, int]:
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket_name, s3_key, local_path)
        file_size = os.path.getsize(local_path)
        return s3_key, file_size
    except NoCredentialsError:
        logger.error(f"Credentials not available for downloading {s3_key}.")
        return None, 0
    except ClientError as e:
        logger.error(f"Error downloading {s3_key}: {e}")
        return None, 0

def pull_file_meta(s3: BaseClient, bucket_name: str, local_folder_path: str, logger: logging.Logger) -> Tuple[Dict[str, int], int, Dict[str, int], int]:
    logger.info('Pulling from S3')
    s3_files, s3_total_size = get_s3_file_list(s3, bucket_name)
    logger.info(f"S3 file count: {len(s3_files)}, Total size: {s3_total_size} bytes")

    logger.info('Pulling from LOCAL')
    local_files, local_total_size = get_local_file_list(local_folder_path)
    logger.info(f"Local file count: {len(local_files)}, Total size: {local_total_size} bytes")

    return s3_files, s3_total_size, local_files, local_total_size

def prep_files_to_download(from_dict: Dict[str, int], to_dict: Dict[str, int]) -> Tuple[Dict[str, int], int]:
    to_download = {key: value for key, value in from_dict.items() if key not in to_dict or value != to_dict[key]}
    total_size = sum(to_download.values())
    return to_download, total_size

def download_files(s3: BaseClient, bucket_name: str, from_dict: Dict[str, int], total_size_from: int, local_folder: str, max_workers: int, logger: logging.Logger):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for s3_key, s3_size in from_dict.items():
            local_file_path = os.path.join(local_folder, s3_key)
            futures.append(executor.submit(download_file, s3, bucket_name, s3_key, local_file_path, logger))

        tqdm_out = TqdmToLogger(logger, level=logging.INFO)
        with tqdm(total=total_size_from, unit='B', unit_scale=True, desc="Downloading", file=tqdm_out) as pbar:
            for future in as_completed(futures):
                try:
                    s3_key, downloaded_size = future.result()
                    if downloaded_size > 0:
                        logger.info(f"Downloaded: {s3_key}, Size: {downloaded_size} bytes")
                    else:
                        logger.error(f"Error: {s3_key}")
                    pbar.update(from_dict[s3_key])
                except Exception as exc:
                    logger.error(f"An error occurred: {exc}")

def get_s3_client(snowball_endpoint: str, aws_access_key_id: str, aws_secret_access_key: str) -> BaseClient:
    return boto3.client('s3',
                        endpoint_url=snowball_endpoint,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

@click.group()
@click.option('--snowball-endpoint', required=True, help='The endpoint URL of the Snowball device.')
@click.option('--aws-access-key-id', envvar='AWS_ACCESS_KEY_ID', help='AWS access key ID.')
@click.option('--aws-secret-access-key', envvar='AWS_SECRET_ACCESS_KEY', help='AWS secret access key.')
@click.option('--log-file', type=click.Path(), help='Path to the log file.')
@click.pass_context
def cli(ctx, snowball_endpoint: str, aws_access_key_id: str, aws_secret_access_key: str, log_file: str):
    ctx.ensure_object(dict)
    ctx.obj['logger'] = setup_logging(log_file)
    ctx.obj['s3'] = get_s3_client(snowball_endpoint, aws_access_key_id, aws_secret_access_key)
    ctx.obj['logger'].info('Welcome to the improved Snowball Copy script!')

@cli.command()
@click.option('--bucket-name', required=True, help='The name of the S3 bucket.')
@click.option('--local-folder', required=True, type=click.Path(exists=True, file_okay=False), help='The local folder to sync files to.')
@click.option('--max-workers', default=1, help='The maximum number of concurrent workers for downloading files.')
@click.option('--dry-run', is_flag=True, help='Perform a dry run without actually downloading files.')
@click.pass_context
def download_s3(ctx, bucket_name: str, local_folder: str, max_workers: int, dry_run: bool):
    logger = ctx.obj['logger']
    s3 = ctx.obj['s3']
    logger.info("Starting Snowball Copy script")

    s3_files, s3_total_size, local_files, local_total_size = pull_file_meta(s3, bucket_name, local_folder, logger)

    to_download, total_to_download = prep_files_to_download(s3_files, local_files)

    if dry_run:
        logger.info(f"Dry run: Would download {len(to_download)} files, total size: {total_to_download} bytes")
    else:
        download_files(s3, bucket_name, to_download, s3_total_size, local_folder, max_workers, logger)

    logger.info("Sync completed.")

@cli.command()
@click.option('--bucket-name', required=True, help='The name of the S3 bucket.')
@click.option('--local-folder', required=True, type=click.Path(exists=True, file_okay=False), help='The local folder to sync files to.')
@click.option('--max-workers', default=1, help='The maximum number of concurrent workers for downloading files.')
@click.option('--dry-run', is_flag=True, help='Perform a dry run without actually downloading files.')
@click.option('--csv-file', required=True, type=click.File(), help='The CSV file to use to filter files from S3')
@click.pass_context
def download_csv(ctx, bucket_name: str, local_folder: str, max_workers: int, dry_run: bool, csv_file):
    logger = ctx.obj['logger']
    s3 = ctx.obj['s3']
    logger.info("Starting Snowball Copy script with CSV filtering")

    s3_files, s3_total_size, local_files, local_total_size = pull_file_meta(s3, bucket_name, local_folder, logger)

    to_download, total_to_download = prep_files_to_download(s3_files, local_files)

    reader = csv.reader(csv_file)
    next(reader, None)  # Skip header
    files_to_download = set([row[0] for row in reader])

    filtered = {key: value for key, value in to_download.items() if key in files_to_download}

    logger.info(f'Filtered files based on CSV down to {len(filtered)} files')

    if dry_run:
        logger.info(f"Dry run: Would download {len(filtered)} files, total size: {sum(filtered.values())} bytes")
    else:
        download_files(s3, bucket_name, filtered, sum(filtered.values()), local_folder, max_workers, logger)

    logger.info("CSV-based sync completed.")

@cli.command()
@click.option('--bucket-name', required=True, help='The name of the S3 bucket.')
@click.option('--csv-file', required=True, help='The local path to new CSV file')
@click.pass_context
def save_csv_from_s3(ctx, bucket_name: str, csv_file: str):
    logger = ctx.obj['logger']
    s3 = ctx.obj['s3']
    logger.info("Starting Snowball Copy script - Save CSV from S3")

    logger.info('Pulling from S3')
    s3_files, s3_total_size = get_s3_file_list(s3, bucket_name)
    logger.info(f"S3 file count: {len(s3_files)}, Total size: {s3_total_size} bytes")

    with open(csv_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['File', 'Size'])

        for file, size in s3_files.items():
            csvwriter.writerow([file, size])

    logger.info(f'CSV file created: {csv_file}')

if __name__ == '__main__':
    cli()

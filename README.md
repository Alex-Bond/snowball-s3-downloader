# Snowball S3 Download Script

A Python utility for efficiently copying files from AWS Snowball S3 to local storage. This script provides file
synchronization with support for concurrent downloads, progress tracking, and CSV-based file filtering.

## Features

- Concurrent file downloads with configurable worker threads
- Progress tracking with size-aware progress bars
- Detailed logging with rotation support
- File integrity verification using size comparison
- CSV-based file filtering
- Dry-run capability for operation verification
- Support for generating CSV manifest of S3 contents

## Prerequisites

- Python 3.x
- Access and secret keys for an AWS Snowball with S3 read permissions
- AWS Snowball device endpoint URL (something like `https://192.168.1.100:8443`)

## Installation

1. Clone this repository or download the script
2. Install required dependencies:

```bash
pip install boto3 click tqdm
```

## Usage

The script provides several commands for different operations:

### Basic S3 Download

Downloads all files from an S3 bucket to a local folder:

```bash
python copy_from_snowball.py --snowball-endpoint <endpoint_url> download-s3 \
    --bucket-name <bucket_name> \
    --local-folder <local_path> \
    --max-workers 4
```

### CSV-Filtered Download

Downloads only files listed in a CSV file:

```bash
python copy_from_snowball.py --snowball-endpoint <endpoint_url> download-csv \
    --bucket-name <bucket_name> \
    --local-folder <local_path> \
    --csv-file <path_to_csv> \
    --max-workers 4
```

### Generate S3 Contents CSV

Creates a CSV file containing all files in an S3 bucket:

```bash
python copy_from_snowball.py --snowball-endpoint <endpoint_url> save-csv-from-s3 \
    --bucket-name <bucket_name> \
    --csv-file <output_csv_path>
```

## Command Options

### Global Options

- `--snowball-endpoint`: (Required) The endpoint URL of the Snowball device
- `--aws-access-key-id`: AWS access key ID (can be set via AWS_ACCESS_KEY_ID environment variable or AWS CLI)
- `--aws-secret-access-key`: AWS secret access key (can be set via AWS_SECRET_ACCESS_KEY environment variable or AWS CLI)
- `--log-file`: Path to the log file (optional)

### Download Options

- `--bucket-name`: (Required) The name of the S3 bucket
- `--local-folder`: (Required) The local folder to sync files to
- `--max-workers`: Maximum number of concurrent download workers (default: 1)
- `--dry-run`: Perform a dry run without actually downloading files
- `--csv-file`: (For download-csv) The CSV file containing the list of files to download

## Logging

Logs include:

- File download progress
- Error messages for failed operations
- Summary statistics for operations
- File counts and total sizes
- 
## Examples

### Basic download with 4 workers:

```bash
python copy_from_snowball.py \
    --snowball-endpoint https://192.168.1.100:8443 \
    --log-file /path/to/logfile.log \
    download-s3 \
    --bucket-name my-bucket \
    --local-folder /path/to/local/folder \
    --max-workers 4
```

### Filtered download using CSV:

```bash
python copy_from_snowball.py \
    --snowball-endpoint https://192.168.1.100:8443 \
    download-csv \
    --bucket-name my-bucket \
    --local-folder /path/to/local/folder \
    --csv-file file_list.csv \
    --max-workers 4
```

### Generate CSV manifest:

```bash
python copy_from_snowball.py \
    --snowball-endpoint https://192.168.1.100:8443 \
    save-csv-from-s3 \
    --bucket-name my-bucket \
    --csv-file bucket_contents.csv
```

## CSV File Format

```csv
File,Size
folder1/file1.txt,1024
folder2/file2.jpg,2048
```

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

from typing import Tuple, Optional
import requests
from requests.exceptions import RequestException
import gzip
import io

def download_s3_range(bucket_url: str, byte_range_start: int, length: int, local_file_path: str) -> Tuple[bool, str]:
    """
    Download a specific byte range from an S3 bucket URL and save it to a local file.
    
    Args:
        bucket_url: The URL of the S3 bucket resource
        byte_range_start: Starting byte position
        length: Number of bytes to download
        local_file_path: Path where the downloaded content will be saved
    
    Returns:
        Tuple[bool, str]: (Success status, Error message if any)
    """
    if not all([bucket_url, byte_range_start >= 0, length > 0, local_file_path]):
        return False, "Invalid input parameters"

    headers = {'Range': f'bytes={byte_range_start}-{byte_range_start + length - 1}'}

    try:
        with requests.get(bucket_url, headers=headers, stream=True) as response:
            response.raise_for_status()
            
            if response.status_code == 206:  # Partial content status code
                with open(local_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True, f"Successfully downloaded {byte_range_start}-{byte_range_start + length - 1} bytes"
            else:
                return False, f"Unexpected status code: {response.status_code}"
                
    except RequestException as e:
        return False, f"Download failed: {str(e)}"
    except IOError as e:
        return False, f"File operation failed: {str(e)}"

def read_s3_range(bucket_url: str, byte_range_start: int, length: int) -> Tuple[Optional[str], str]:
    """
    Download and decompress a gzipped byte range from an S3 bucket URL.
    
    Args:
        bucket_url: The URL of the S3 bucket resource
        byte_range_start: Starting byte position
        length: Number of bytes to download
    
    Returns:
        Tuple[Optional[str], str]: (Decompressed content if successful, Error message if any)
    """
    byte_range_start = int(byte_range_start) if isinstance(byte_range_start, str) else byte_range_start
    length = int(length) if isinstance(length, str) else length
    
    if not all([bucket_url, byte_range_start >= 0, length > 0]):
        return None, "Invalid input parameters"

    headers = {'Range': f'bytes={byte_range_start}-{byte_range_start + length - 1}'}

    try:
        with requests.get(bucket_url, headers=headers, stream=True) as response:
            response.raise_for_status()
            
            if response.status_code == 206:  # Partial content status code
                # Create in-memory buffer and decompress
                compressed_data = io.BytesIO(response.content)
                with gzip.GzipFile(fileobj=compressed_data, mode='rb') as gz:
                    decompressed_content = gz.read().decode('utf-8')
                return decompressed_content, "Success"
            else:
                return None, f"Unexpected status code: {response.status_code}"
                
    except RequestException as e:
        return None, f"Download failed: {str(e)}"
    except (IOError, gzip.GzipError) as e:
        return None, f"Decompression failed: {str(e)}"

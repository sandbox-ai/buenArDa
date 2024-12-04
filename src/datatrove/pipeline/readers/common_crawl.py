from typing import TYPE_CHECKING, Callable, Literal, List, Optional, Dict
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.io import DataFileLike, DataFolderLike
import logging
from warcio.archiveiterator import ArchiveIterator
import io

from typing import Tuple, Optional
import requests
from requests.exceptions import RequestException
import gzip
import requests
from urllib.parse import quote_plus
import json

def search_commoncrawl_index(
    pattern: str,
    index_name: str = 'CC-MAIN-2024-33',
    server: str = 'http://index.commoncrawl.org/'
) -> Optional[List[Dict]]:
    """
    Search the Common Crawl index for a URL pattern and return matching records.
    
    Args:
        pattern: URL pattern to search for
        index_name: Common Crawl index name (e.g., 'CC-MAIN-2024-33')
        server: Common Crawl index server URL
    
    Returns:
        List of dictionaries containing matching records or None if request fails
    """
    try:
        encoded_url = quote_plus(pattern)
        index_url = f'{server}{index_name}-index?url={encoded_url}&output=json'
        
        headers = {
            'user-agent': 'cc-get-started/1.0 (Common Crawl Index Search Bot)'
        }
        
        response = requests.get(index_url, headers=headers)
        response.raise_for_status()
        
        # Parse JSONL response into list of dictionaries
        records = [
            json.loads(record) 
            for record in response.text.strip().split('\n')
            if record.strip()
        ]
        
        logging.info(f"Found {len(records)} records matching pattern: {pattern}")
        return records

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to search Common Crawl index: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse response: {str(e)}")
        return None

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

class CommonCrawlReader(BaseDiskReader):
    """Read data directly from Common Crawl using the index API.
    Will read each record as a separate document.

    Args:
        data_folder: Folder to store temporary files (required by BaseDiskReader but not used)
        index_name: Common Crawl index name (e.g., 'CC-MAIN-2024-33')
        url_pattern: URL pattern to search for in Common Crawl index
        server: Common Crawl index server URL
        compression: the compression to use (default: "infer")
        limit: limit the number of documents to read
        skip: skip the first n rows
        file_progress: show progress bar for files
        doc_progress: show progress bar for documents
        adapter: function to adapt the data dict
        text_key: the key containing the text data
        id_key: the key containing the id for each sample
        default_metadata: additional metadata for all samples
    """

    name = "🌐 CommonCrawl"
    _requires_dependencies = ["warcio", ("cchardet", "faust-cchardet"), ("magic", "python-magic")]

    def __init__(
        self,
        data_folder: DataFolderLike,
        index_name: str = 'CC-MAIN-2024-33',
        url_pattern: str = "*",
        server: str = 'http://index.commoncrawl.org/',
        compression: Literal["infer", "gzip", "zstd"] | None = "infer",
        limit: int = -1,
        skip: int = 0,
        file_progress: bool = False,
        doc_progress: bool = False,
        adapter: Callable = None,
        text_key: str = "text",
        id_key: str = "id",
        default_metadata: dict = None,
    ):
        self.index_name = index_name
        self.url_pattern = url_pattern
        self.server = server
        self.compression = compression
        super().__init__(
            data_folder,
            paths_file=None,
            limit=limit,
            skip=skip,
            file_progress=file_progress,
            doc_progress=doc_progress,
            adapter=adapter,
            text_key=text_key,
            id_key=id_key,
            default_metadata=default_metadata,
            recursive=False,
            glob_pattern=None,
            shuffle_files=False,
        )

    def read_file(self, filepath: str):
        from datatrove.pipeline.readers.warc import process_record

        # Search Common Crawl index
        records = search_commoncrawl_index(
            pattern=self.url_pattern,
            index_name=self.index_name,
            server=self.server
        )
        
        if not records:
            logging.warning(f"No records found for pattern: {self.url_pattern}")
            return

        for record_info in records:
            content, error = read_s3_range(
                record_info['filename'], 
                record_info['offset'], 
                record_info['length']
            )
            
            if not content:
                logging.warning(f"Failed to fetch record: {error}")
                continue
                
            # Create a file-like object for ArchiveIterator
            stream = io.BytesIO(content.encode('utf-8'))
            
            # Process each WARC record like WarcReader does
            try:
                for ri, record in enumerate(ArchiveIterator(stream)):
                    with self.track_time():
                        # Use the same process_record function from warc.py
                        extracted_data = process_record(record)
                        if not extracted_data:
                            continue
                            
                        # Add any additional CommonCrawl-specific metadata
                        extracted_data.update({
                            'offset': record_info['offset'],
                            'length': record_info['length'],
                            'filename': record_info['filename']
                        })
                        
                        document = self.get_document_from_dict(
                            extracted_data,
                            record_info['filename'],
                            ri
                        )
                        if document:
                            yield document
            except Exception as e:
                logging.warning(f"Error processing record from {record_info['filename']}: {str(e)}")
                continue
import requests
from urllib.parse import quote_plus
import json
from typing import List, Optional, Dict
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time, random

def get_commoncrawl_indexes():
    try:
        response = requests.get('https://index.commoncrawl.org/collinfo.json')
        response.raise_for_status()
        indexes = response.json()
        return [index['id'] for index in indexes]
    except Exception as e:
        print(f"Error fetching indexes: {str(e)}")
        return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)

def search_with_retry(pattern, index_name):
    results = search_commoncrawl_index(pattern, index_name=index_name)
    if results is None:
        raise ValueError(f"No results returned for index {index_name}")
    return results

def search_commoncrawl_index(
    pattern: str,
    index_name: str = 'CC-MAIN-2024-33',
    server: str = 'http://index.commoncrawl.org/'
) -> Optional[List[Dict]]:
    """
    Search the Common Crawl index for a URL pattern and return matching records.
    Includes automatic retry with exponential backoff and jitter.
    
    Args:
        pattern: URL pattern to search for
        index_name: Common Crawl index name (e.g., 'CC-MAIN-2024-33')
        server: Common Crawl index server URL
    
    Returns:
        List of dictionaries containing matching records or None if request fails
    """
    try:
        # Add initial random delay to help avoid thundering herd
        time.sleep(random.uniform(0, 2))
        
        encoded_url = quote_plus(pattern)
        index_url = f'{server}{index_name}-index?url={encoded_url}&output=json'
        
        headers = {
            'user-agent': 'cc-get-started/1.0 (Common Crawl Index Search Bot)'
        }
        
        response = requests.get(index_url, headers=headers)
        
        if response.status_code == 503:
            logging.warning("Received 503 error, will retry with backoff")
            raise requests.exceptions.RequestException("Service temporarily unavailable")
            
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

# Example usage:
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = search_commoncrawl_index("commoncrawl.org/*")
    if results:
        print(json.dumps(results, indent=2))

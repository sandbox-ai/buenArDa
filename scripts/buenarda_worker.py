import json
import os
import argparse
import re
from scripts.get_cc_range import read_cc_range
from scripts.search_commoncrawl_index import search_commoncrawl_index
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import signal
import sys
import trafilatura

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_existing_urls(output_file):
    if not os.path.exists(output_file):
        return set()
    with open(output_file, 'r') as f:
        try:
            data = [json.loads(line) for line in f if line.strip()]
            return {item['url'] for item in data}
        except json.JSONDecodeError:
            return set()

def append_content(output_file, url, content):
    with open(output_file, 'a') as f:
        json_line = json.dumps({'url': url, 'content': content})
        f.write(json_line + '\n')

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_cc_content(filename, offset, length):
    return read_cc_range(
        'https://data.commoncrawl.org/'+filename,
        offset,
        length
    )

def handle_shutdown(signum, frame):
    logger.info("Received shutdown signal, cleaning up...")
    sys.exit(0)

def clean_content(content):
    if not content:
        return None
    
    # Remove JavaScript
    content = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', content)
    
    # Remove style tags
    content = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', content)
    
    # Remove HTML tags while keeping their content
    content = re.sub(r'<[^>]+>', ' ', content)
    
    # Remove extra whitespace and newlines
    content = re.sub(r'\s+', ' ', content)
    
    # Remove special HTML entities
    content = re.sub(r'&[a-zA-Z0-9#]+;', ' ', content)
    
    # Clean unicode spaces and separators
    content = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', content)
    
    content = content.strip()
    
    # Return None if content is too short after cleaning
    return content if len(content) > 50 else None

def process_index(index_name, output_file, pattern="*.ar", worker_id=0, total_workers=1):
    if not index_name or not output_file:
        raise ValueError("Invalid index_name or output_file")

    signal.signal(signal.SIGTERM, handle_shutdown)
    
    existing_urls = load_existing_urls(output_file)
    
    try:
        # Convert index name to uppercase for CommonCrawl URL
        index_for_url = index_name.upper()
        results = search_commoncrawl_index(pattern, index_name=index_for_url)
        logger.info(f"Found {len(results)} matching results in {index_name}")
        
        worker_results = [r for i, r in enumerate(results) if i % total_workers == worker_id]
        logger.info(f"Worker {worker_id}/{total_workers} processing {len(worker_results)} results")
        
        success_count = 0
        error_count = 0
        
        for result in worker_results:
            url = result['url']
            if url in existing_urls:
                continue
                
            try:
                content, _ = fetch_cc_content(
                    result['filename'],
                    result['offset'],
                    result['length']
                )
                # Trafilatura initial cleaning
                content = trafilatura.extract(content)
                if content:
                    # Additional cleaning of html and js
                    content = clean_content(content)
                    if content:
                        append_content(output_file, url, content)
                        success_count += 1
                        if success_count % 100 == 0:
                            logger.info(f"Processed {success_count} URLs successfully")
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing {url}: {str(e)}")
                
        logger.info(f"Completed processing. Successes: {success_count}, Errors: {error_count}")
    except Exception as e:
        logger.error(f"Critical error processing index {index_name}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', required=True, help='CommonCrawl index to process')
    parser.add_argument('--output', required=True, help='Output file path')
    parser.add_argument('--pattern', default="*.ar", help='URL pattern to search for (default: *.ar)')
    parser.add_argument('--worker-id', type=int, default=0, help='Worker ID for chunking')
    parser.add_argument('--total-workers', type=int, default=1, help='Total number of workers')
    args = parser.parse_args()
    
    process_index(args.index, args.output, args.pattern, args.worker_id, args.total_workers)

if __name__ == "__main__":
    main()
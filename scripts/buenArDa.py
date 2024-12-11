import json
import os
import argparse
import requests
from scripts.get_s3_range import read_s3_range
from scripts.search_commoncrawl_index import search_commoncrawl_index, get_commoncrawl_indexes
import trafilatura

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

def main(output_file, pattern):
    # Load existing URLs to skip
    existing_urls = load_existing_urls(output_file)

    # Get all available indexes
    indexes = get_commoncrawl_indexes()
    if not indexes:
        print("No indexes found. Exiting.")
        return

    print(f"Found {len(indexes)} CommonCrawl indexes")

    # Process each index
    for index in indexes:
        print(f"Processing index: {index}")
        results = search_commoncrawl_index(pattern, index_name=index)
        print(f"Found {len(results)} matching results in {index}")
        
        # Process results incrementally
        for result in results:
            url = result['url']
            if url in existing_urls:
                print(f"Skipping existing URL: {url}")
                continue
                
            try:
                content, result = read_s3_range('https://data.commoncrawl.org/'+result['filename'], 
                                              result['offset'], result['length'])
                content = trafilatura.extract(content)
                if content:
                    append_content(output_file, url, content)
                    print(f"Saved content for: {url}")
                else:
                    print(f"No content retrieved for: {url}")
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process CommonCrawl indexes.')
    parser.add_argument('--output_file', type=str, required=True, help='Output file to save the data')
    parser.add_argument('--pattern', type=str, default="*.ar", help='URL pattern to search for (default: *.ar)')

    args = parser.parse_args()
    main(args.output_file, args.pattern)


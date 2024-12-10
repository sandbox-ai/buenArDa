
import json
import os
import argparse
from scripts.get_s3_range import read_s3_range
from scripts.search_commoncrawl_index import search_commoncrawl_index

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

def process_index(index_name, output_file):
    # Load existing URLs
    existing_urls = load_existing_urls(output_file)
    
    # Process single index
    results = search_commoncrawl_index("*.ar", index_name=index_name)
    print(f"Found {len(results)} matching results in {index_name}")
    
    for result in results:
        url = result['url']
        if url in existing_urls:
            continue
            
        try:
            content, _ = read_s3_range(
                'https://data.commoncrawl.org/'+result['filename'],
                result['offset'],
                result['length']
            )
            if content:
                append_content(output_file, url, content)
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', required=True, help='CommonCrawl index to process')
    parser.add_argument('--output', required=True, help='Output file path')
    args = parser.parse_args()
    
    process_index(args.index, args.output)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import json
import argparse
import shutil
from datetime import datetime

def merge_data_files(data_dir, output_file):
    """Merge all jsonl files into a single file"""
    seen_urls = set()
    total_records = 0
    
    with open(output_file, 'w') as outfile:
        for filename in os.listdir(data_dir):
            if not filename.endswith('.jsonl'):
                continue
                
            filepath = os.path.join(data_dir, filename)
            with open(filepath) as infile:
                for line in infile:
                    data = json.loads(line)
                    if data['url'] not in seen_urls:
                        outfile.write(line)
                        seen_urls.add(data['url'])
                        total_records += 1
    
    print(f"Merged {total_records} unique records into {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Manage crawler data')
    parser.add_argument('--data-dir', required=True, help='Data directory path')
    parser.add_argument('--merge', help='Merge all files into specified output file')
    
    args = parser.parse_args()
    
    if args.merge:
        merge_data_files(args.data_dir, args.merge)

if __name__ == "__main__":
    main()
import requests

def download_s3_range(bucket_url, byte_range_start, length, local_file_path):
    # Set the range of bytes you want to download
    byte_range_end = byte_range_start + length
    headers = {'Range': f'bytes={byte_range_start}-{byte_range_end-1}'}

    # Send the GET request with the range header
    response = requests.get(bucket_url, headers=headers)

    if response.status_code == 206:  # Partial content status code
        # Write the content to a local file
        with open(local_file_path, 'wb') as f:
            f.write(response.content)

        print(f"Downloaded bytes {byte_range_start}-{byte_range_end-1} to {local_file_path}")
    else:
        print(f"Failed to download range. HTTP Status Code: {response.status_code}")

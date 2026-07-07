import requests
from urllib.parse import urlparse
import os
from logging_utils.env import load_root_env
load_root_env()
from time import sleep

IMAGES_URLS_FILE = os.getenv('image_url_file') # List of image URLs to be scraped and downloaded will be in the form of a text file
                # Each URL should be on a new line
                # prob need to read from the text file and populate this list

OUTPUT_DIR = os.getenv('Images_urls_output')
TIMEOUT = 15

#+--------------------------------------------------------------------------+#
# Script to scrape and download images related to product designs form urls ##
#+--------------------------------------------------------------------------+#

def download_image(url):

    try:
        
        ressponse = requests.get(url, stream=True, timeout=TIMEOUT)
        ressponse.raise_for_status()
        
        parse_image = urlparse(url)
        file_name = parse_image.path.split('/')[-1]
        
        if not file_name:
            raise ValueError(f"Could not determine file name from URL {url}.")

        file_path = os.path.join(OUTPUT_DIR, file_name)

        with open(file_path, 'wb') as file:
            for chunk in ressponse.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        
        print(f"Downloaded image | {file_name} | from | {url} |")

    except Exception as e:
        print(f"Error downloading image from {url}: {e}")


def load_image_urls(file_path):
    with open(file_path, 'r') as file:
        urls = [line.strip() for line in file if line.strip()]
    return urls




# main execution

print("*"*50)
print("Starting image download process...")
sleep(1)
print("Ensuring output directory exists...")
sleep(1)
if not os.path.exists(OUTPUT_DIR):
    print(f"Output directory {OUTPUT_DIR} does not exist. Creating it...")
    sleep(0.5)
    os.makedirs(OUTPUT_DIR)
    print(f"Created output directory {OUTPUT_DIR}.")
    sleep(0.5)
else: 
    print(f"Output directory found")
    sleep(1)
print("Gathering image URLs from file...")
sleep(1)
IMAGES_URLS = load_image_urls(IMAGES_URLS_FILE)
print(f"Total images to download: {len(IMAGES_URLS)}")
sleep(1)
print("Downloading images...")
print("*"*50)
sleep(0.5)
for url in IMAGES_URLS:
    download_image(url)
sleep(0.5)
print("|","*"*67,"|")
print("|"," "*16,"Image download process completed."," "*16,"|")
print("|","*"*67,"|")





"""Download product design images from a list of URLs.

    gtc-affinity-images

Reads one URL per line from IMAGE_URLS_FILE (default: scripts/scripts input/Images_urls.txt)
and saves each image into IMAGE_OUTPUT_DIR, named after the last part of the URL.
Existing files with the same name are overwritten.
"""
from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import requests

from gtc_common import env
from gtc_common.log import setup_logging

logger = logging.getLogger(__name__)
TIMEOUT = 15


def download_image(url: str, output_dir: Path) -> bool:
    file_name = urlparse(url).path.split("/")[-1]
    if not file_name:
        logger.error("Could not work out a file name from %s", url)
        return False
    try:
        response = requests.get(url, stream=True, timeout=TIMEOUT)
        response.raise_for_status()
        with open(output_dir / file_name, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                fh.write(chunk)
        logger.info("Downloaded %s", file_name)
        return True
    except Exception as exc:
        logger.error("Failed %s: %s", url, exc)
        return False


def main() -> None:
    setup_logging("affinity_image_scraper")
    urls_file = env.path("IMAGE_URLS_FILE", "scripts/scripts input/Images_urls.txt")
    output_dir = env.path("IMAGE_OUTPUT_DIR")
    output_dir.mkdir(parents=True, exist_ok=True)
    urls = [line.strip() for line in urls_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    logger.info("Downloading %s images into %s", len(urls), output_dir)
    ok = sum(download_image(url, output_dir) for url in urls)
    logger.info("Done: %s downloaded, %s failed", ok, len(urls) - ok)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
IMDb Data Download Script

Downloads IMDb TSV data files for local development and testing.
"""

import os
import requests
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IMDb data URLs (these are the actual URLs from IMDb)
IMDB_URLS = {
    'title.basics.tsv.gz': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title.ratings.tsv.gz': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
    'name.basics.tsv.gz': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    'title.principals.tsv.gz': 'https://datasets.imdbws.com/title.principals.tsv.gz'
}

def download_file(url, filepath):
    """Download a file from URL to filepath."""
    try:
        logger.info(f"Downloading {url} to {filepath}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        return False

def extract_gzip(gzip_path, extract_path):
    """Extract gzipped file."""
    import gzip
    import shutil
    
    try:
        logger.info(f"Extracting {gzip_path} to {extract_path}")
        with gzip.open(gzip_path, 'rb') as f_in:
            with open(extract_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        logger.info(f"Extracted {extract_path}")
        return True
    except Exception as e:
        logger.error(f"Error extracting {gzip_path}: {str(e)}")
        return False

def main():
    """Main execution function."""
    # Create data directory
    data_dir = Path("/opt/data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Downloading IMDb data to {data_dir}")
    
    success_count = 0
    total_files = len(IMDB_URLS)
    
    for filename, url in IMDB_URLS.items():
        gzip_path = data_dir / filename
        tsv_path = data_dir / filename.replace('.gz', '')
        
        # Download gzipped file
        if download_file(url, gzip_path):
            # Extract gzipped file
            if extract_gzip(gzip_path, tsv_path):
                # Remove gzipped file to save space
                gzip_path.unlink()
                success_count += 1
                logger.info(f"Successfully processed {filename}")
            else:
                logger.error(f"Failed to extract {filename}")
        else:
            logger.error(f"Failed to download {filename}")
    
    logger.info(f"Download completed: {success_count}/{total_files} files successful")
    
    if success_count == total_files:
        logger.info("All IMDb data files downloaded successfully!")
        return 0
    else:
        logger.error("Some files failed to download")
        return 1

if __name__ == "__main__":
    exit(main())

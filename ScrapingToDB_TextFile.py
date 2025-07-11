from bs4 import BeautifulSoup
import requests
import Connexion
import json
import time
import logging
import signal
import sys
import random

# Configure logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def fetch_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            time.sleep(random.uniform(1, 3))
            return response
        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                raise
            wait_time = 2 ** attempt + random.uniform(0, 1)
            logging.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

def validate_listing_data(info):
    """Validate scraped data before DB insertion"""
    if not info[0].strip():
        raise ValueError("Empty location")
    
    # More flexible price validation for test data
    price_clean = info[2].replace('$','').replace(',','').strip()
    if not (price_clean.isdigit() or price_clean in ['', 'Not specified']):
        raise ValueError(f"Invalid price format: {info[2]}")
        
    if len(info) < 8:
        raise ValueError("Incomplete data")

DEFAULT_VALUE = 'Not specified'

# Load config from file
try:
    with open('config.json') as f:
        config = json.load(f)
    url = config['url']
except Exception as e:
    logging.error(f"Error loading config: {e}")
    exit(1)

# Fetch page with error handling
try:
    page = fetch_with_retry(url)
    if page is None:
        raise ValueError("Failed to fetch page after retries")
    time.sleep(2)
    soup = BeautifulSoup(page.text, 'html.parser')
    tables = soup.find_all('table')
except Exception as e:
    logging.error(f"Scraping failed: {e}")
    exit(1)

db = Connexion.Dbconnect()
with open('data.txt', 'w') as f:
    try:
        tables = soup.find_all('table')
        total_items = sum(len(table.find_all('tr'))-1 for table in tables)
        logging.info(f"Starting to process {total_items} listings")
        
        processed = 0
        for table in tables:
            rows = table.find_all('tr')[1:]  # Skip headers
            for row in rows:
                processed += 1
                if processed % 5 == 0:
                    logging.info(f"Processing item {processed}/{total_items}")
                
                cols = row.find_all('td')
                if len(cols) >= 3:  # Need at least 3 columns
                    # Map test data to our structure with mock values
                    info = [
                        f"{cols[0].text.strip()} {cols[1].text.strip()}",  # Full name as location
                        "For Sale",  # Mock status
                        str(random.randint(200000, 800000)),  # Random price
                        "Test Owner",  # Mock owner
                        str(random.randint(1, 5)),  # Random beds
                        str(random.randint(1, 3)),  # Random baths
                        str(random.randint(800, 3000)),  # Random sqft
                        str(random.randint(4000, 10000))  # Random lot size
                    ]
                    
                    try:
                        sql = "INSERT INTO house(location,status,price,owner,bed,bath,sqft,sqft_lot) VALUES "+str(tuple(info))
                        db.dbcursor.execute(sql)
                        db.commit_db()
                        f.write('; '.join(info) + '\n')
                    except Exception as e:
                        logging.warning(f"DB error: {e}")
    
    except Exception as e:
        logging.error(f"Processing failed: {e}")
    finally:
        db.close_db()

def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    logging.info("Shutdown signal received. Cleaning up...")
    try:
        if 'db' in globals():
            db.close_db()
            logging.info("Database connection closed.")
    except Exception as e:
        logging.error(f"Error during shutdown: {e}")
    sys.exit(0)

# Register shutdown handler at startup
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


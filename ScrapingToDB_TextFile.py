from bs4 import BeautifulSoup
import requests
import Connexion
import json
import time
import logging
import signal
import sys

# Configure logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                raise
            wait_time = 2 ** attempt
            logging.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
            time.sleep(wait_time)

DEFAULT_VALUE = 'Not specified'

# Load config from file
try:
    with open('config.json') as f:
        config = json.load(f)
    url = config['url']
    headers = config['headers']
except Exception as e:
    logging.error(f"Error loading config: {e}")
    exit(1)

# Fetch page with error handling
try:
    page = fetch_with_retry(url, headers)
    if page is None:
        raise ValueError("Failed to fetch page after retries")
    time.sleep(2)
    soup = BeautifulSoup(page.text, 'html.parser')
    lists = soup.find_all('div', class_="jsx-2775064451 fallBackImgWrap")
except Exception as e:
    logging.error(f"Scraping failed: {e}")
    exit(1)

db = Connexion.Dbconnect()
with open('data.txt', 'w') as f:
    total_items = len(lists)
    logging.info(f"Starting to process {total_items} listings")
    
    for idx, list_item in enumerate(lists, 1):
        if idx % 10 == 0 or idx == total_items:  # Log every 10 items and at the end
            logging.info(f"Processing item {idx}/{total_items}")
            
        if list_item != None:
            try:
                location = list_item.find('div', class_="jsx-1982357781 address ellipsis srp-page-address srp-address-redesign")
                price = list_item.find('span', class_="Price__Component-rui__x3geed-0 gipzbd")
                status = list_item.find('span', class_="jsx-3853574337 statusText")
                ow = list_item.find_all('span', class_="jsx-287440024")
                owner = ow[1]
                infos = list_item.find_all('span', class_="jsx-946479843 meta-value")
                for i in range(len(infos)):
                    infos[i] = infos[i].text if infos[i] != None else DEFAULT_VALUE
                location = location.text if location != None else DEFAULT_VALUE
                price = price.text if price != None else DEFAULT_VALUE
                owner = owner.text if owner != None else DEFAULT_VALUE
                status = status.text if status != None else DEFAULT_VALUE
                info = [location, status, price, owner]
                for i in range(len(infos)):
                    info.append(infos[i])
                if len(infos) < 4:
                    for i in range(len(infos), 4):
                        info.append("NoV")
                
                def validate_listing_data(info):
                    """Validate scraped listing data before DB insertion"""
                    if not info[0].strip():  # Location
                        raise ValueError("Empty location")
                    if not info[2].replace(',', '').replace('.', '').isdigit():  # Price
                        raise ValueError(f"Invalid price: {info[2]}")
                    if len(info) < 8:  # Ensure all fields exist
                        raise ValueError("Incomplete listing data")

                # Validate before DB insertion
                try:
                    validate_listing_data(info)
                    sql = "INSERT INTO house(location,status,price,owner,bed,bath,sqft,sqft_lot) VALUES "+str(tuple(info))
                    db.dbcursor.execute(sql)
                    db.commit_db()
                except ValueError as e:
                    logging.warning(f"Invalid listing data: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"DB error: {e}")
                    continue
            except Exception as e:
                logging.warning(f"Error processing listing: {e}")
                continue

            for i in range(len(info)):
                f.write(info[i])
                f.write("; ")
            f.write('\n')

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


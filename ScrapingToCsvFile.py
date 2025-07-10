from bs4 import BeautifulSoup
import requests
from csv import writer
import logging
import sys

# Configure logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DEFAULT_VALUE = 'Not specified'

def get_user_input():
    location = input("Enter location (e.g., 'Stockton_CA'): ").strip()
    output_file = input("Enter output filename (default: housing.csv): ").strip() or "housing.csv"
    return location, output_file

def fetch_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1"
    }
    try:
        logging.info(f"Fetching URL: {url}")
        page = requests.get(url, headers=headers, timeout=10)
        page.raise_for_status()
        return page
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch URL: {e}")
        sys.exit(1)

def extract_listing_data(list_item):
    try:
        location = list_item.find('div', class_="jsx-1982357781 address ellipsis srp-page-address srp-address-redesign")
        price = list_item.find('span', class_="Price__Component-rui__x3geed-0 gipzbd")
        status = list_item.find('span', class_="jsx-3853574337 statusText")
        ow = list_item.find_all('span', class_="jsx-287440024")
        owner = ow[1] if len(ow) > 1 else None
        infos = list_item.find_all('span', class_="jsx-946479843 meta-value")
        
        info = [
            location.text if location else DEFAULT_VALUE,
            status.text if status else DEFAULT_VALUE,
            price.text.strip('$').replace(',', '') if price else DEFAULT_VALUE,
            owner.text if owner else DEFAULT_VALUE
        ]
        
        for i in range(len(infos)):
            info.append(infos[i].text if infos[i] else DEFAULT_VALUE)
        
        if len(infos) < 4:
            for i in range(len(infos), 4):
                info.append("NoV")
        
        return info
    except Exception as e:
        logging.warning(f"Error processing listing: {e}")
        return None

def save_to_csv(output_file, listings):
    try:
        with open(output_file, 'w', encoding='utf8', newline='') as f:
            thewriter = writer(f)
            header = ['Location', 'Status', 'Price', 'Owner', 'Bed', 'Bath', 'SQFT', 'SQFT_LOT']
            thewriter.writerow(header)
            
            for listing in listings:
                if listing is not None:
                    thewriter.writerow(listing)
                    
        logging.info(f"Successfully saved data to {output_file}")
    except IOError as e:
        logging.error(f"File operation failed: {e}")
        sys.exit(1)

def main():
    try:
        location, output_file = get_user_input()
        url = f"https://www.realtor.com/realestateandhomes-search/{location}/show-newest-listings"
        
        page = fetch_page(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        lists = soup.find_all('div', class_="jsx-2775064451 fallBackImgWrap")
        
        listings = []
        for list_item in lists:
            listing_data = extract_listing_data(list_item)
            if listing_data:
                listings.append(listing_data)
                
        save_to_csv(output_file, listings)

    except Exception as e:
        logging.critical(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()



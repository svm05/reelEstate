from bs4 import BeautifulSoup
import requests
import Connexion
import json
import time

DEFAULT_VALUE = 'Not specified'

# Load config from file
try:
    with open('config.json') as f:
        config = json.load(f)
    url = config['url']
    headers = config['headers']
except Exception as e:
    print(f"Error loading config: {e}")
    exit(1)

# Fetch page with error handling
try:
    page = requests.get(url, headers=headers, timeout=10)
    page.raise_for_status()
    time.sleep(2)  # Rate limiting
    soup = BeautifulSoup(page.text, 'html.parser')
    lists = soup.find_all('div', class_="jsx-2775064451 fallBackImgWrap")
except requests.exceptions.RequestException as e:
    print(f"Failed to fetch URL: {e}")
    exit(1)

db = Connexion.Dbconnect()
with open('data.txt', 'w') as f:
    for list in lists:
        if list != None:
            try:
                location = list.find('div', class_="jsx-1982357781 address ellipsis srp-page-address srp-address-redesign")
                price = list.find('span', class_="Price__Component-rui__x3geed-0 gipzbd")
                status = list.find('span', class_="jsx-3853574337 statusText")
                ow = list.find_all('span', class_="jsx-287440024")
                owner = ow[1]
                infos = list.find_all('span', class_="jsx-946479843 meta-value")
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
                sql = "INSERT INTO house(location,status,price,owner,bed,bath,sqft,sqft_lot) VALUES "+str(tuple(info))
                db.dbcursor.execute(sql)
                db.commit_db()
            except Exception as e:
                print(f"Error processing listing: {e}")
                continue

            for i in range(len(info)):
                f.write(info[i])
                f.write("; ")
            f.write('\n')

db.close_db()


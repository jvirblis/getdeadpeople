import requests
import json
import time
import csv
import logging
from datetime import datetime

INPUT_FILE = "sample_person.csv"
OUTPUT_FILE = "results.json"
LOG_FILE = "script.log"
API_URL = "https://notariat.ru/api/probate-cases/"
RATE_LIMIT = 150  # API allows 150 calls per hour
SLEEP_TIME = 3600 / RATE_LIMIT  # Sleep time between requests to avoid exceeding limit
SAVE_INTERVAL = 100  # Save results every 5 minutes (300 seconds)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

results = []
last_save_time = time.time()

def read_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def query_api(person):
    payload = {"name": f"{person['family_name']} {person['name']} {person['patronymic']}"}    
    response = requests.post(API_URL, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error {response.status_code} for {payload['name']}")
        return {}

def save_results():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as jsonfile:
        json.dump(results, jsonfile, ensure_ascii=False, indent=4)
    logging.info("Results saved successfully.")

def main():
    global last_save_time
    persons = read_csv(INPUT_FILE)
    request_count = 0
    
    for person in persons:
        if request_count >= RATE_LIMIT:
            logging.info("Rate limit reached. Waiting an hour...")
            time.sleep(3600)
            request_count = 0
        
        api_response = query_api(person)
        request_count += 1
        
        if "records" in api_response:
            for record in api_response["records"]:
                record.update({
                    "person_id": person["id"],
                    "family_name": person["family_name"],
                    "name": person["name"],
                    "patronymic": person["patronymic"],
                    "death_date": person["death_date"]
                })
                results.append(record)
        else:
            logging.warning(f"No records found for {person['family_name']} {person['name']} {person['patronymic']}")
        
        time.sleep(SLEEP_TIME)  # Respect API rate limit
        
        # Save results every 5 minutes
        if time.time() - last_save_time > SAVE_INTERVAL:
            save_results()
            last_save_time = time.time()
    
    save_results()  # Final save at the end
    logging.info(f"Final results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

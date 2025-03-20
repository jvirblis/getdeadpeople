import requests
import json
import time
import csv
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

INPUT_FILE = "sample_person.csv"
OUTPUT_FILE = "results.json"
LOG_FILE = "script.log"
API_URL = "https://notariat.ru/api/probate-cases"
SAVE_INTERVAL = 300  # Save results every 5 minutes (300 seconds)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')
results = []
last_save_time = time.time()


def read_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def query_api(person):
    payload = {
        "name": f"{person['family_name']} {person['name']} {person['patronymic']}",
        "birth_date": datetime.strptime(person['birth_date'], "%Y-%m-%d").strftime("%Y%m%d"), #преображаем данные из формата базы в формат, который принимает АПИ
        "death_date": "NULL" #вероятно, АПИ принимает запрос только целиком, пропустить дату смерти нельзя
        #todo: в случае наличия даты смерти в нашей БД можно сначала смотреть с ней, если результата нет - без нее. Сверять по дате рождения 
    }
    
    try:
        response = requests.post(API_URL, json=payload, timeout=10)
        if response.status_code == 200:
            return response.json().get("records", [])
        else:
            logging.error(f"Error {response.status_code} for {payload['name']}")
    except requests.RequestException as e:
        logging.error(f"Request failed for {payload['name']}: {e}")
    return []

def save_results():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as jsonfile:
        json.dump(results, jsonfile, ensure_ascii=False, indent=4)
    logging.info("Results saved successfully.")

def process_person(person):
    records = query_api(person)
    for record in records:
        record["person_id"] = person["id"]
        record["family_name"] = person["family_name"]
        record["name"] = person["name"]
        record["patronymic"] = person["patronymic"]
        record["birth_date"] = person["birth_date"]
        results.append(record)

def main():
    global last_save_time
    persons = read_csv(INPUT_FILE)
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for _ in executor.map(process_person, persons):
            time.sleep(1)  # Small delay to prevent simultaneous overload
            
            if time.time() - last_save_time > SAVE_INTERVAL:
                save_results()
                last_save_time = time.time()
    
    save_results()  # Final save at the end
    logging.info(f"Final results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
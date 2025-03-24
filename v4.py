import requests
import json
import time
import csv
import logging
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import deque

INPUT_FILE = "sample_person.csv"
OUTPUT_FILE = "results.json"
LOG_FILE = "script.log"
PROXIES_FILE = "proxies.csv"  # Fichier contenant la liste des proxies
API_URL = "https://notariat.ru/api/probate-cases"
TEST_URL = "https://httpbin.org/ip"  # URL pour tester les proxies
SAVE_INTERVAL = 300  # Save results every 5 minutes (300 seconds)
REQUESTS_PER_HOUR = 150  # Limit per hour per IP
REQUEST_INTERVAL = 1  # 1 second between requests

# Configuration de la journalisation
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
results = []
last_save_time = time.time()

def load_proxies_from_csv(file_path):
    """Charge les proxies depuis un fichier CSV"""
    proxies = []
    try:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) > 0:
                    proxy = row[0].strip()
                    # Vérifier si le proxy commence par http:// ou https://
                    if not (proxy.startswith('http://') or proxy.startswith('https://')):
                        proxy = f"http://{proxy}"
                    proxies.append(proxy)
        logging.info(f"Chargé {len(proxies)} proxies depuis {file_path}")
        return proxies
    except Exception as e:
        logging.error(f"Erreur lors du chargement des proxies depuis {file_path}: {e}")
        return []

def validate_proxy(proxy):
    """Vérifie si un proxy fonctionne"""
    try:
        proxies = {
            "http": proxy,
            "https": proxy
        }
        response = requests.get(TEST_URL, proxies=proxies, timeout=5)
        if response.status_code == 200:
            logging.info(f"Proxy validé: {proxy}")
            return True
        else:
            logging.warning(f"Proxy non valide (code {response.status_code}): {proxy}")
            return False
    except Exception as e:
        logging.warning(f"Proxy non fonctionnel: {proxy}. Erreur: {e}")
        return False

def validate_proxies(proxies, max_workers=10):
    """Valide une liste de proxies en parallèle"""
    valid_proxies = []
    
    logging.info(f"Validation de {len(proxies)} proxies...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(validate_proxy, proxies))
        
        for proxy, is_valid in zip(proxies, results):
            if is_valid:
                valid_proxies.append(proxy)
    
    logging.info(f"Validation terminée. {len(valid_proxies)} proxies valides sur {len(proxies)}")
    return valid_proxies

# Gestion des proxies et des limites de taux
class ProxyManager:
    def __init__(self, proxies, requests_per_hour):
        self.proxies = proxies
        self.proxy_usage = {proxy: {"count": 0, "last_reset": time.time()} for proxy in proxies}
        self.proxy_queue = deque(proxies)
        self.requests_per_hour = requests_per_hour
        self.lock = ThreadPoolExecutor(max_workers=1)
    
    def get_proxy(self):
        """Récupère le prochain proxy disponible"""
        def _get_proxy():
            if not self.proxies:
                return None
                
            current_time = time.time()
            
            # Vérifier tous les proxies pour réinitialiser les compteurs si nécessaire
            for proxy, data in self.proxy_usage.items():
                if current_time - data["last_reset"] > 3600:  # 1 heure
                    data["count"] = 0
                    data["last_reset"] = current_time
            
            # Trouver un proxy disponible
            for _ in range(len(self.proxy_queue)):
                proxy = self.proxy_queue.popleft()
                if self.proxy_usage[proxy]["count"] < self.requests_per_hour:
                    self.proxy_queue.append(proxy)
                    return proxy
                else:
                    # Si le proxy a atteint sa limite, le remettre à la fin de la file
                    self.proxy_queue.append(proxy)
            
            # Si aucun proxy n'est disponible, retourner None
            return None
        
        # Utiliser un verrou pour éviter les problèmes de concurrence
        future = self.lock.submit(_get_proxy)
        return future.result()
    
    def increment_usage(self, proxy):
        """Incrémente le compteur d'utilisation pour un proxy"""
        def _increment():
            if proxy in self.proxy_usage:
                self.proxy_usage[proxy]["count"] += 1
        
        future = self.lock.submit(_increment)
        return future.result()
    
    def mark_proxy_failed(self, proxy):
        """Marque un proxy comme défaillant et le retire de la liste"""
        def _mark_failed():
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                del self.proxy_usage[proxy]
                
                # Reconstruire la file d'attente
                self.proxy_queue = deque(self.proxies)
                logging.warning(f"Proxy marqué comme défaillant et retiré: {proxy}")
        
        future = self.lock.submit(_mark_failed)
        return future.result()

def read_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def query_api(person, proxy_manager):
    payload = {
        "name": f"{person['family_name']} {person['name']} {person['patronymic']}",
        "birth_date": datetime.strptime(person['birth_date'], "%Y-%m-%d").strftime("%Y%m%d"),
        "death_date": "NULL"
    }
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        proxy = proxy_manager.get_proxy()
        
        if proxy is None:
            logging.warning("Tous les proxies ont atteint leur limite ou sont épuisés. Attente de 60 secondes.")
            time.sleep(60)  # Attendre avant de réessayer
            retry_count += 1
            continue
        
        try:
            proxies = {
                "http": proxy,
                "https": proxy
            }
            
            response = requests.post(
                API_URL, 
                json=payload, 
                proxies=proxies,
                timeout=10
            )
            
            # Incrémenter l'utilisation du proxy
            proxy_manager.increment_usage(proxy)
            
            if response.status_code == 200:
                return response.json().get("records", [])
            elif response.status_code == 429:  # Too Many Requests
                logging.warning(f"Rate limit atteint pour le proxy {proxy}. Essai avec un autre proxy.")
                retry_count += 1
                time.sleep(1)  # Petit délai avant de réessayer
            elif response.status_code >= 500:  # Erreur serveur
                logging.error(f"Erreur serveur {response.status_code} pour {payload['name']} avec proxy {proxy}")
                retry_count += 1
                time.sleep(2)  # Délai un peu plus long
            else:
                logging.error(f"Erreur {response.status_code} pour {payload['name']} avec proxy {proxy}")
                retry_count += 1
        except requests.RequestException as e:
            logging.error(f"Requête échouée pour {payload['name']} avec proxy {proxy}: {e}")
            # Marquer le proxy comme défaillant si l'erreur indique un problème de connexion
            if isinstance(e, (requests.ConnectTimeout, requests.ConnectionError)):
                proxy_manager.mark_proxy_failed(proxy)
            retry_count += 1
    
    logging.error(f"Échec après {max_retries} tentatives pour {payload['name']}")
    return []

def save_results():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as jsonfile:
        json.dump(results, jsonfile, ensure_ascii=False, indent=4)
    logging.info(f"Résultats sauvegardés avec succès. {len(results)} enregistrements.")

def process_person(args):
    person, proxy_manager = args
    records = query_api(person, proxy_manager)
    result_records = []
    
    for record in records:
        record_with_person = {
            "person_id": person["id"],
            "family_name": person["family_name"],
            "name": person["name"],
            "patronymic": person["patronymic"],
            "birth_date": person["birth_date"]
        }
        record_with_person.update(record)
        result_records.append(record_with_person)
    
    return result_records

def main():
    global last_save_time, results
    
    # Charger et valider les proxies
    all_proxies = load_proxies_from_csv(PROXIES_FILE)
    if not all_proxies:
        logging.error("Aucun proxy trouvé dans le fichier. Arrêt du script.")
        return
    
    valid_proxies = validate_proxies(all_proxies)
    if not valid_proxies:
        logging.error("Aucun proxy valide trouvé. Arrêt du script.")
        return
    
    # Initialiser le gestionnaire de proxies avec les proxies validés
    proxy_manager = ProxyManager(valid_proxies, REQUESTS_PER_HOUR)
    
    # Charger les personnes à traiter
    persons = read_csv(INPUT_FILE)
    logging.info(f"Chargé {len(persons)} personnes à traiter")
    
    # Préparer les arguments pour process_person
    person_args = [(person, proxy_manager) for person in persons]
    
    # Traiter les personnes
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for args in person_args:
            # Ajouter un délai entre les soumissions de tâches
            futures.append(executor.submit(process_person, args))
            time.sleep(REQUEST_INTERVAL)
            
            # Vérifier si nous devons sauvegarder les résultats intermédiaires
            current_time = time.time()
            if current_time - last_save_time > SAVE_INTERVAL:
                # Récupérer les résultats des tâches terminées
                for future in [f for f in futures if f.done()]:
                    try:
                        records = future.result()
                        results.extend(records)
                        futures.remove(future)
                    except Exception as e:
                        logging.error(f"Erreur lors de la récupération des résultats: {e}")
                
                save_results()
                last_save_time = current_time
                logging.info(f"Proxies restants: {len(proxy_manager.proxies)}")
        
        # Récupérer tous les résultats restants
        for future in futures:
            try:
                records = future.result()
                results.extend(records)
            except Exception as e:
                logging.error(f"Erreur lors de la récupération des résultats: {e}")
    
    save_results()  # Sauvegarde finale
    logging.info(f"Traitement terminé. {len(results)} enregistrements sauvegardés dans {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()

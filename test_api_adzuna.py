import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

BASE_URL = "https://api.adzuna.com/v1/api/jobs/fr/search"

params = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "what": "data engineer",
    "results_per_page": 10,
    "content-type": "application/json"
}

total_jobs = 0

for page in range(1, 6):  # teste les 5 premières pages
    print(f"\n--- PAGE {page} ---")

    url = f"{BASE_URL}/{page}"

    response = requests.get(url, params=params)

    print("Status:", response.status_code)

    data = response.json()

    results = data.get("results", [])

    print("Nombre d'offres sur cette page:", len(results))

    total_jobs += len(results)

    # afficher la première offre de la page pour inspection
    if results:
        print("Exemple d'offre :")
        print(json.dumps(results[0], indent=2))
    else:
        print("Plus de résultats, arrêt de la pagination.")
        break

print("\nTotal d'offres récupérées:", total_jobs)
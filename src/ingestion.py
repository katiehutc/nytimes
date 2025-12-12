import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURATION ---
API_KEY = "MKyesAaNHHea0smCGlNCQ9x45gvfxLpr" 
BASE_URL = "https://api.nytimes.com/svc/archive/v1/{year}/{month}.json"

# fetches the entire archive for a specific month
def fetch_month_archive(year, month):

    url = BASE_URL.format(year=year, month=month)
    params = {'api-key': API_KEY}
    
    print(f"Fetching Archive: {year}-{month:02d} ...")
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 429:
            print("Rate limit hit. Waiting 60 seconds...")
            time.sleep(60)
            response = requests.get(url, params=params)

        response.raise_for_status()
        data = response.json()
        
        docs = data['response']['docs']
        print(f"  -> Found {len(docs)} articles.")
        return docs

    except Exception as e:
        print(f"Error fetching {year}-{month}: {e}")
        return []

def clean_archive_data(docs):
    cleaned = []
    for doc in docs:
        if doc.get('type_of_material') not in ['News', 'Article', 'Review', 'Op-Ed']: # filter for just "News" type material
            continue
            
        cleaned.append({
            'date': doc.get('pub_date'),
            'headline': doc.get('headline', {}).get('main', ''),
            'abstract': doc.get('abstract'),
            'keywords': [k['value'] for k in doc.get('keywords', [])], 
            'section': doc.get('section_name')
        })
    return cleaned

if __name__ == "__main__":
    all_data = []
    
    years = range(2020, 2025) 

    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    for year in years:
        for month in range(1, 13):
            if year > current_year or (year == current_year and month > current_month):
                break

            raw_docs = fetch_month_archive(year, month)
            clean_docs = clean_archive_data(raw_docs)
            all_data.extend(clean_docs)
            
            print(f"  -> Total collected so far: {len(all_data)}")
            
            time.sleep(5)

    df = pd.DataFrame(all_data)
    filename = "nyt_2020_2024.csv"
    df.to_csv(filename, index=False)
    
    print(f"Done! Saved {len(df)} articles to {filename}")
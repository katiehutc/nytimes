import requests
import pandas as pd
import time
import os

API_KEY = "MKyesAaNHHea0smCGlNCQ9x45gvfxLpr" 
BASE_URL = "https://api.nytimes.com/svc/archive/v1/{year}/{month}.json"

# Fetches the entire archive for a specific month
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
        if doc.get('type_of_material') not in ['News', 'Article', 'Review', 'Op-Ed']: # Filter for just "News" type material
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
    all_year_data = []
    
    # Loop through all 12 months of 2024
    for month in range(1, 13):
        raw_docs = fetch_month_archive(2024, month)
        clean_docs = clean_archive_data(raw_docs)
        all_year_data.extend(clean_docs)
        
        time.sleep(10)

    # Save to CSV
    df = pd.DataFrame(all_year_data)
    df.to_csv("nyt_2024_full_year.csv", index=False)
    
    print("------------------------------------------------")
    print(f"Done! Saved {len(df)} articles to nyt_2024_full_year.csv")

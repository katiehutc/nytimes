import requests
import pandas as pd
import time
import os

# --- CONFIG ---
API_KEY = "MKyesAaNHHea0smCGlNCQ9x45gvfxLpr"
CURRENT_FILE = "nyt_2020_2024.csv" # Your existing file
BASE_URL = "https://api.nytimes.com/svc/archive/v1/{year}/{month}.json"

def fetch_month(year, month):
    url = BASE_URL.format(year=year, month=month)
    params = {'api-key': API_KEY}
    try:
        resp = requests.get(url, params=params)
        if resp.status_code == 429:
            time.sleep(60)
            resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json().get('response', {}).get('docs', [])
    except Exception as e:
        print(f"Error {year}-{month}: {e}")
        return []

def clean_docs(docs):
    cleaned = []
    for doc in docs:
        if doc.get('type_of_material') not in ['News', 'Article', 'Review', 'Op-Ed']:
            continue
        cleaned.append({
            'date': doc.get('pub_date'),
            'headline': doc.get('headline', {}).get('main', ''),
            'abstract': doc.get('abstract'),
            'section': doc.get('section_name'),
            'url': doc.get('web_url')
        })
    return cleaned

if __name__ == "__main__":
    # The new years you want
    years = [2015, 2016, 2017, 2018, 2019]
    
    print(f"--- Fetching 2015-2019 ---")
    
    # We will collect these in a list first
    new_data = []
    
    for year in years:
        for month in range(1, 13):
            print(f"Fetching {year}-{month:02d}...")
            raw = fetch_month(year, month)
            clean = clean_docs(raw)
            new_data.extend(clean)
            time.sleep(6) # Be nice to API

    # --- MERGING STRATEGY ---
    print("Merging with existing file...")
    
    # 1. Load the new stuff into a DF
    df_old = pd.DataFrame(new_data)
    
    # 2. Load your CURRENT 2020-2024 data
    if os.path.exists(CURRENT_FILE):
        df_current = pd.read_csv(CURRENT_FILE)
        
        # 3. Combine: Old data on top, Current data on bottom
        df_final = pd.concat([df_old, df_current], ignore_index=True)
        
        # 4. Save as a new "Master" file
        df_final.to_csv("nyt_2015_2024_full.csv", index=False)
        print(f"✅ SUCCESS! Created 'nyt_2015_2024_full.csv' with {len(df_final)} records.")
    else:
        print("❌ Could not find your existing 2020-2024 file to merge with.")
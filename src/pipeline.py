from prefect import flow, task
import pandas as pd
import duckdb
import os
from bertopic import BERTopic
from umap import UMAP

CSV_FILE = "nyt_2015_2024_full.csv"
DB_FILE = "nyt_analysis_2.duckdb"

@task
def get_data():
    if os.path.exists(CSV_FILE):
        print("get_data done, returning csv")
        return pd.read_csv(CSV_FILE)
    else:
        raise FileNotFoundError(f"Could not find {CSV_FILE}. Please run ingestion.py first")

@task
def transform_and_model(df):
    # error check for empty headlines / abstracts, fill with empty string
    df['headline'] = df['headline'].fillna('') 
    df['abstract'] = df['abstract'].fillna('')
    
    # combine headline and abstract for more context to train model
    docs = (df['headline'] + ". " + df['abstract']).tolist()
    
    # pick 20,000 random articles to train model
    from sklearn.utils import shuffle
    training_docs = shuffle(docs, random_state=42)[:20000] 
    
    # a topic must have at least 150 articles
    topic_model = BERTopic(min_topic_size=50, 
                           language="english",
                           umap_model=UMAP(n_neighbors=10, random_state=42),
                           verbose=True)
    topic_model.fit(training_docs)
    
    #transform in chunks of 5000
    chunk_size = 5000
    all_topics = []
    
    for i in range(0, len(docs), chunk_size):
        chunk = docs[i : i + chunk_size]
        # transform chunk
        chunk_topics, _ = topic_model.transform(chunk)
        all_topics.extend(chunk_topics)
    
    # attach headline to topic
    df['topic_id'] = all_topics

    # get the table of topics / topic ID and convert it to a dictionary
    topic_info = topic_model.get_topic_info()
    topic_map = topic_info.set_index('Topic')['Name'].to_dict()

    # match each topic id to the dictionary
    df['topic_name'] = df['topic_id'].map(topic_map)
    
    return df

@task
def load_to_duckdb(df):
    conn = duckdb.connect(DB_FILE)
    conn.execute("CREATE OR REPLACE TABLE processed_articles AS SELECT * FROM df")
    conn.close()
    print("Loaded to DuckDB")

@flow(name="NYT-Pipeline")
def main_flow():
    raw_df = get_data()
    
    final_df = transform_and_model(raw_df)
    
    load_to_duckdb(final_df)

if __name__ == "__main__":
    main_flow()
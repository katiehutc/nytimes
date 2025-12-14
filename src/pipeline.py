import os
import re
import pandas as pd
import duckdb
from prefect import flow, task
from bertopic import BERTopic
from umap import UMAP
from sentence_transformers import SentenceTransformer
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
nltk.download("stopwords")
nltk.download("wordnet")

CSV_FILE = "nyt_2015_2024_full.csv"
DB_FILE = "nyt_analysis_2.duckdb"

@task
def get_data():
    if os.path.exists(CSV_FILE):
        print("get_data done, returning csv")
        return pd.read_csv(CSV_FILE)
    else:
        raise FileNotFoundError(f"Could not find {CSV_FILE}. Please run ingestion.py first")

stop = set(stopwords.words("english"))
lemm = WordNetLemmatizer()

def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^\w\s\-]", " ", text)  # keep hyphens + words
    text = re.sub(r"\s+", " ", text)
    return text.strip()


@task
def transform_and_model(df):
    # error check for empty headlines / abstracts, fill with empty string
    df['headline'] = df['headline'].fillna('') 
    df['abstract'] = df['abstract'].fillna('')
    
    
    # clean & combine headline and abstract for more context to train model
    df["clean"] = (df["headline"] + ". " + df["abstract"]).fillna("").apply(clean_text)
    docs = df["clean"].tolist()

    
    # pick 20,000 random articles to train model
    training_df = df.sample(n=20000, random_state=42)
    training_df = training_df[training_df["headline"].str.len() > 10]
    training_docs = training_df["clean"].tolist()

    embedding_model = SentenceTransformer("all-mpnet-base-v2")

    umap_model = UMAP(
        n_neighbors=50,
        n_components=50,
        min_dist=0.0,
        metric="cosine",
        random_state=42
    )

    # a topic must have at least 200 articles
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        min_topic_size=40,
        language="english",
        verbose=True
    )
    topic_model.fit(training_docs)
    topic_info = topic_model.get_topic_info()
    num_topics = topic_info.shape[0] - 1 # Exclude Topic -1 (Noise)
    
    print("-" * 50)
    print(f"✅ MODEL TRAINING COMPLETE. Topics Found: {num_topics}")
    print(topic_info.head(20))
    print("-" * 50)
    
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
    print(topic_info.head(20))
    print("Total topics:", topic_info.shape[0])
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
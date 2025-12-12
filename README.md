# New York Times Archives Topics Analysis and Visualization
\
## Project Overview

This project implements a robust Extract, Transform, Load (ETL) pipeline using **Prefect** to process a large data set of over 400,000 New York Times article headlines and abstracts. The core goal is to identify and visualize major cultural and political trends over the last decade using **BERTopic** for unsupervised topic modeling, ultimately loading the results into a fast **DuckDB** database for interactive analysis via **Streamlit**.

### Architecture Diagram

The pipeline consists of three main stages orchestrated by Prefect:



## 🛠️ Technology Stack

* **Orchestration:** Prefect 2.x (Flows & Tasks)
* **Topic Modeling:** BERTopic (leveraging UMAP, HDBSCAN, and Sentence-BERT)
* **Data Storage:** DuckDB (In-process analytical database)
* **Data Analysis:** Pandas
* **Visualization:** Streamlit & Plotly
* **Environment:** Miniforge/Conda (for dependency stability)

---

## 🚀 Getting Started

### Prerequisites

You must have **Miniforge** (or Miniconda) installed on your system to manage the complex Python dependencies reliably.

### Installation & Setup

1.  **Clone the Repository:**
    ```bash
    git clone [(https://github.com/katiehutc/nytimes.git)]
    cd [nytimes]
    ```

2.  **Create and Activate the Conda Environment:**
    We use a dedicated environment and the Conda-Forge channel to ensure stable binary compatibility for the machine learning libraries.

    ```bash
    # Create the environment with a stable Python version
    conda create -n nyt-env python=3.11 -y

    # Activate the new environment
    conda activate nyt-env
    ```

3.  **Install Dependencies:**
    Install all core libraries (including the complex ML stack) using a single Conda command for guaranteed compatibility:

    ```bash
    conda install -c conda-forge pandas python-duckdb prefect bertopic scikit-learn=1.2.2 umap-learn=0.5.3 -y
    ```
    *Note: Explicitly setting the versions for `scikit-learn` and `umap-learn` resolves known binary incompatibility errors on macOS.*

---

## Running the Pipeline

The project requires two terminal windows to run simultaneously: one for the Prefect monitoring server and one for the flow execution.

### 1. Start the Prefect Server (Terminal 1)

Open your first terminal, ensure `nyt-env` is active, and start the local orchestration server:

```bash
prefect server start

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_FILE = "nyt_analysis_2.duckdb"

st.title("NYT Topic Modeling Dashboard")

st.markdown("""
★Purpose: This interactive dashboard visualizes topic trends across ~400,000 New York Times articles from 2015-2024.

★This data was obtained from the New York Times Archives API and processed through Prefect and BERTopic

★Key Features:
★ Topic Volume: Analyze the total number of articles published per topic over the entire period.
★ Temporal Trends: View how the volume of selected topics changes year-over-year.
★ Topic Details: Use the filters to drill down into specific, manually labeled news categories like 'COVID-19', 'Ukraine War', and 'Macroeconomics'.
""")

st.divider()

st.set_page_config(page_title="New York Times Analysis (2015-2024)", layout="wide")
st.markdown("---")

@st.cache_data # data only loaded once
def load_data():

    try:
        conn = duckdb.connect(DB_FILE, read_only=True)
        # count the number of articles per topic, grouped by month
        query = f"""
        SELECT 
            topic_name,
            date_trunc('month', CAST(date AS TIMESTAMP)) as month,
            count(*) as count
        FROM processed_articles
        WHERE topic_id != -1 
        GROUP BY 1, 2
        ORDER BY 2
        """
        df = conn.execute(query).df()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading DuckDB data. Have you run pipeline.py yet? Error: {e}")
        return pd.DataFrame()

# load the aggregated data
df = load_data()

if not df.empty:
    st.sidebar.header("Filter Trends")

    df['topic_name'] = df['topic_name'].fillna('Unclassified').astype(str)
    df['display_topic'] = df['topic_name']
    all_display_topics = sorted(df['display_topic'].unique())

    initial_filter = [t for t in all_display_topics if t not in ['Noise Random Stuff', 'Abstract', 'Main']] # filter
    
    # allow user to select multiple topics
    selected_topics = st.sidebar.multiselect(
        "Select Topics to Compare", 
        all_display_topics, 
        default=initial_filter[:0]
    )

    # MAIN CHART 
    if selected_topics:
        filtered_df = df[df['display_topic'].isin(selected_topics)] # filter based on user selection
        
        # Create interactive line chart with Plotly
        fig = px.line(
            filtered_df, 
            x='month', 
            y='count', 
            color='display_topic',
            title="Topic Frequency Over Time (2015-2024)",
            labels={'count': 'Number of Articles', 'month': 'Date', 'display_topic': 'Topic'},
            template="plotly_white",
            height=600
        )
        
        # Customize chart appearance
        fig.update_xaxes(dtick="M12", tickformat="%Y") # Show labels annually
        fig.update_traces(mode="lines", hovertemplate="%{y} articles<br>%{x|%b %Y}")
        fig.update_layout(hovermode="x unified")
        
        st.plotly_chart(fig, use_container_width=True)

        # INSIGHTS 
        st.subheader("Key Insights")
        cols = st.columns(3)
        
        for idx, topic in enumerate(selected_topics):
            topic_data = filtered_df[filtered_df['display_topic'] == topic]
            
            # Find the peak month and total count for the topic
            total_articles = topic_data['count'].sum()
            peak_row = topic_data.sort_values('count', ascending=False).iloc[0]
            peak_month_str = peak_row['month'].strftime('%b %Y')
            peak_count = peak_row['count']
            
            with cols[idx % 3]:
                st.metric(
                    label=f"Total: {topic}", 
                    value=f"{total_articles:,} Articles", 
                    delta=f"Peak: {peak_month_str} ({peak_count:,} articles)"
                )
    
    else:
        st.info("Please select one or more topics from the sidebar to visualize the trends.")

else:
    st.warning("Data is empty. Please check the pipeline logs for errors.")
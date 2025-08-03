Real-Time Market Intelligence System for Indian Stocks
======================================================

This project is a Python-based system designed to scrape, process, and analyze real-time discussions about the Indian stock market from Twitter/X. It converts textual data into quantitative signals that could be used for algorithmic trading insights.

🏛️ Architecture
----------------

The system operates in a three-stage pipeline:

1.  **Data Ingestion**: Scrapes tweets using tweepy for specified hashtags (#nifty50, #sensex, etc.).
    
2.  **ETL Processing**: Cleans the raw data, calculates sentiment and engagement scores, deduplicates records, and stores the processed data in the efficient Parquet format.
    
3.  **Analysis & Visualization**: A Jupyter notebook loads the final data to perform time-series analysis and generate memory-efficient visualizations.
    

✨ Features
----------

*   **Concurrent Scraping**: Utilizes multi-threading to scrape multiple hashtags simultaneously for better performance.
    
*   **Efficient Storage**: Saves processed data as Parquet files for fast analytical queries and low storage footprint.
    
*   **Text-to-Signal Conversion**: Implements a basic sentiment analysis and combines it with engagement metrics to generate a composite trading signal.
    
*   **Scalable & Documented**: The codebase is modular, documented, and includes considerations for scaling.
    

🚀 Setup and Execution
----------------------

### 1\. Prerequisites

*   Python 3.8+
    

### 2\. Installation

Clone the repository and set up a virtual environment:

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML``   git clone   cd market-intelligence-system  # Create and activate a virtual environment  python -m venv venv  source venv/bin/activate  # On Windows, use `venv\Scripts\activate`  # Install dependencies  pip install -r requirements.txt   ``

### 3\. Running the Pipeline

Execute the main script to run the entire data collection and processing pipeline:
`   python main.py   `

This will:

1.  Scrape the latest 2000 tweets from the last 24 hours.
    
2.  Save the raw data to data/raw\_tweets.csv.
    
3.  Process the raw data, generate signals, and save the final output to data/signals.parquet.
    

### 4\. Analyzing the Results

Open and run the Jupyter notebook to explore the data and visualize the results:
`   jupyter notebook notebooks/analysis.ipynb   `

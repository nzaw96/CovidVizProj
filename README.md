# Project Description

This repo contains all the code for the data visualization project where I built a dashboard that shows the impact of COVID-19 on different US states (and territories) and their corresponding responses. This is an end-to-end project: 1) data extraction from <a href="https://health.google.com/covid-19/open-data/raw-data">COVID-19 Open Data Source</a> and data preprocessing in Jupyter Notebook using Python, 2) Staging and Loading the processed data into tables in a Snowflake database and 3) Writing custom queries, defining logical relationships between tables and building an interative dashboard in Tableau.
<br><br>
The interactive dashboard has been shared to Tableau Public and can be accessed <a href="https://public.tableau.com/app/profile/nay.zaw.aung.win/viz/Covid-19InDifferentUSStates/COVIDDASHBOARD?publish=yes">HERE</a>.
<br><br>

# File Description In This Repo

1. <em>covidProjNotebook.ipynb</em> contains the python code for data ingestion from the source and data preprocessing (dropping/filling null values, dropping duplicate rows, selecting the desired columns, etc.) so that the processed files can then be loaded into Snowflake. The notebook goes processing each file/dataframe at a time and includes detailed comments.

2. <em>stagingAndLoading.sql</em> file contains all the code for creating databases and tables in Snowflake and SnowSQL (CLI) commands for staging and loading data into those tables.

3. <em>customSqlQueriesInTableau.sql</em> file contains the custom SQl queries, in the form of CTEs, used in creating the dashboard.

# Start project
1. Config airflow
```bash
mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
2. Create `prepared_csv` for result file
```bash
mkdir prepared_csv
```
3. Start docker-compose
```bash
docker-compose up -d
```
4. Go to Airflow UI `localhost:8080` and start dag `project_dag`. Login: admin, password: admin 
5. To look at the finished tables
```bash
docker exec -it postgres psql -U admin data
```

# Final tables
`top_artists_by_country`: the most popular artist in each country  
`top_genres_by_country`: number of songs in genres by country  
`top_songs_by_countries`: the most popular song in each country  
`top_songs_in_more_than_1_country`: songs that was top 1 in more than one country  
`top_songs_more_than_1_time`: songs that was top 1 more than one time  
`total_and_max_streams_by_country`: max streams and total streams by country  

# Additional info
- In file `kaggle_data_url.txt` link to original dataset. The dataset in this project is about 2 times smaller
- In folder `additional files/` stores:
  - `.ipynb` file with data preprocessing in Jupyter for testing
  - `load_to_postgres.py` to load `charts.csv` and `country_aliases.csv` to postgres
  - `sql_to_csv.py` to load sql tables `charts` and `country_aliases` to csv files

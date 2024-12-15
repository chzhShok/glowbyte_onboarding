from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
import os

def data_transformation(
        spark: SparkSession,
        data: DataFrame, 
        country_aliases: DataFrame, 
        ) -> tuple[DataFrame, DataFrame]:
    data = data.dropna(subset=["name"])
    logging.info("Drop 'name'")
    data = data.withColumn("duration_minutes", col("duration") / (1000 * 60))
    logging.info("Make 'duration_minutes'")

    country_aliases = country_aliases.select(col("alias"), col("country").alias("country_name"))

    data.createOrReplaceTempView("data")
    country_aliases.createOrReplaceTempView("country_aliases")

    data = spark.sql(
    """
    select *
    from data
    join country_aliases on data.country = country_aliases.alias
    """
    )

    logging.info("Join countrt_aliases and charts")
    columns_to_drop = ['country', 'alias', 'duration']
    data = data.drop(*columns_to_drop)
    logging.info("Drop extra columns")

    return data, country_aliases
 

def create_new_dataframes(spark: SparkSession, data: DataFrame, dataframes: dict) -> dict:
    data.createOrReplaceTempView("data")

    total_and_max_streams_by_country = spark.sql(
    """
    select 
        country_name, 
        sum(streams) as total_streams, 
        max(streams) as max_stream, 
        count(*) as count_songs 
    from data
    group by country_name 
    order by total_streams desc
    """
    )
    logging.info("Create total_and_max_streams_by_country")

    top_genres_by_country = spark.sql(
    """
    select 
        country_name, 
        genre, 
        count(*) as genre_count
    from (
        select 
            country_name, 
            explode(
                split(replace(replace(replace(artist_genres, "]", ""), "[", ""), "'", ""), ',\\s*')
            ) as genre
        from data
    ) exploded_genres
    group by country_name, genre
    order by genre_count desc
    """
    )
    logging.info("Create top_genres_by_country")

    top_songs_by_countries = spark.sql(
    """
    select country_name, name
    from (
        select 
            name, 
            country_name, 
            row_number() over(partition by country_name order by position) as rank
        from data
    ) as ranked_songs
    where rank = 1
    """
    )
    logging.info("Create top_songs_by_countries")

    top_artists_by_country = spark.sql(
    """
    with artists_streams as
    (
        select 
            artist,
            sum(streams) as total_streams,
            country_name
        from
        (
            select 
                explode(split(replace(replace(replace(artists, "]", ""), "[", ""), "'", ""), ',\\s*')) as artist,
                streams, 
                country_name
            from data
        ) as artist_data
        group by artist, country_name
    )

    select 
        country_name,
        artist,
        total_streams
    from (
        select 
            country_name,
            artist,
            total_streams,
            row_number() over (partition by country_name order by total_streams desc) as rank
        from artists_streams
    ) as ranked_artists
    where rank = 1
    order by country_name
    """
    )
    logging.info("Create top_artists_by_country")

    top_songs_more_than_1_time = spark.sql(
    """
    with song_position_1 as
    (
        select 
            name, 
            artists,
            row_number() over(partition by name order by name) as rank
        from (
            select
                name, 
                artists
            from data
            where position = 1
        ) 
    )

    select
        name,
        artists
    from
    (
        select 
            max(rank) as max_rank,
            name,
            artists
        from song_position_1
        group by name, artists
    )
    where max_rank > 1
    order by name
    """
    )
    logging.info("Create top_songs_more_than_1_time")

    top_songs_in_more_than_1_country = spark.sql(
    """
    select distinct
        name, 
        artists,
        count(country_name) as total_countries
    from data
    where position = 1
    group by name, artists
    having total_countries > 1
    order by name
    """
    )
    logging.info("Create top_songs_in_more_than_1_country")

    dataframes['total_and_max_streams_by_country'] = total_and_max_streams_by_country
    dataframes['top_genres_by_country'] = top_genres_by_country
    dataframes['top_songs_by_countries'] = top_songs_by_countries
    dataframes['top_artists_by_country'] = top_artists_by_country
    dataframes['top_songs_more_than_1_time'] = top_songs_more_than_1_time
    dataframes['top_songs_in_more_than_1_country'] = top_songs_in_more_than_1_country
    logging.debug("Add all to dataframes")
    
    return dataframes


def write_to_csv(dataframes: dict, path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    for name, df in dataframes.items():
        try:
            df.write.format('csv') \
                .option("header", "true") \
                .save(f"{path}/{name}.csv", mode='overwrite')
            logging.info(f"Write {name}: {path}/{name}.csv")
        except Exception as e:
            logging.error(f"Failed to write {name}: {e}")
            raise


def spark_process():
    spark = SparkSession.builder.appName("spark") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "2") \
        .config("spark.master", "local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()
    logging.info("Spark init")

    dataframes = dict()

    csv_path = "/opt/airflow/csv"

    country_aliases = spark.read.option("header", True).csv(f"{csv_path}/country_aliases.csv")
    logging.info("Read country_aliases.csv")
    data = spark.read.option("header", True).csv(f"{csv_path}/charts.csv")
    logging.info("Read charts.csv")
    
    data, country_aliases = data_transformation(spark, data, country_aliases)
    logging.info("Data transformed")

    dataframes = create_new_dataframes(spark, data, dataframes)
    logging.info("Created new databases")

    prepared_path = "/opt/airflow/prepared_csv"

    # dataframes['charts'] = data
    # dataframes['country_aliases'] = country_aliases

    write_to_csv(dataframes, prepared_path)
    logging.info("New data wrote to csv")

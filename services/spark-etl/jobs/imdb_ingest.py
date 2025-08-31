#!/usr/bin/env python3
"""
IMDb Data Ingestion Job

This Spark job processes IMDb TSV data files and loads them into Iceberg tables
following a bronze/silver/gold data architecture.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session with Iceberg support."""
    return (SparkSession.builder
            .appName("IMDb-Ingestion")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
            .config("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def load_imdb_data(spark, data_path):
    """Load IMDb TSV data files."""
    logger.info(f"Loading IMDb data from: {data_path}")
    
    # Define schemas for IMDb tables
    title_basics_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", BooleanType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), True),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ])
    
    title_ratings_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", DoubleType(), True),
        StructField("numVotes", IntegerType(), True)
    ])
    
    name_basics_schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", IntegerType(), True),
        StructField("deathYear", IntegerType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])
    
    title_principals_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])
    
    # Load data with proper schemas
    title_basics = spark.read.csv(
        f"{data_path}/title.basics.tsv", 
        sep='\t', 
        header=True, 
        schema=title_basics_schema,
        nullValue="\\N"
    )
    
    title_ratings = spark.read.csv(
        f"{data_path}/title.ratings.tsv", 
        sep='\t', 
        header=True, 
        schema=title_ratings_schema,
        nullValue="\\N"
    )
    
    name_basics = spark.read.csv(
        f"{data_path}/name.basics.tsv", 
        sep='\t', 
        header=True, 
        schema=name_basics_schema,
        nullValue="\\N"
    )
    
    title_principals = spark.read.csv(
        f"{data_path}/title.principals.tsv", 
        sep='\t', 
        header=True, 
        schema=title_principals_schema,
        nullValue="\\N"
    )
    
    return {
        'title_basics': title_basics,
        'title_ratings': title_ratings,
        'name_basics': name_basics,
        'title_principals': title_principals
    }

def create_bronze_tables(spark, dataframes):
    """Create bronze layer tables (raw data as-is)."""
    logger.info("Creating bronze layer tables...")
    
    # Bronze: Raw data as-is
    dataframes['title_basics'].writeTo("iceberg.bronze.title_basics").overwrite()
    dataframes['title_ratings'].writeTo("iceberg.bronze.title_ratings").overwrite()
    dataframes['name_basics'].writeTo("iceberg.bronze.name_basics").overwrite()
    dataframes['title_principals'].writeTo("iceberg.bronze.title_principals").overwrite()
    
    logger.info("Bronze tables created successfully")

def create_silver_tables(spark, dataframes):
    """Create silver layer tables (cleaned and validated data)."""
    logger.info("Creating silver layer tables...")
    
    # Silver: Cleaned and validated data
    title_basics_clean = (dataframes['title_basics']
        .filter(col("titleType").isin("movie", "tvMovie", "tvSeries", "tvMiniSeries"))
        .filter(col("isAdult") == False)
        .filter(col("startYear").isNotNull())
        .filter(col("startYear") >= 1900)
        .withColumn("genres_array", split(col("genres"), ","))
        .drop("genres")
        .withColumnRenamed("genres_array", "genres"))
    
    title_ratings_clean = (dataframes['title_ratings']
        .filter(col("averageRating").isNotNull())
        .filter(col("numVotes") >= 10))  # Minimum votes threshold
    
    name_basics_clean = (dataframes['name_basics']
        .filter(col("primaryName").isNotNull())
        .withColumn("professions_array", split(col("primaryProfession"), ","))
        .drop("primaryProfession")
        .withColumnRenamed("professions_array", "professions"))
    
    title_principals_clean = (dataframes['title_principals']
        .filter(col("category").isin("actor", "actress", "director", "writer", "producer"))
        .filter(col("nconst").isNotNull()))
    
    # Write silver tables
    title_basics_clean.writeTo("iceberg.silver.title_basics").overwrite()
    title_ratings_clean.writeTo("iceberg.silver.title_ratings").overwrite()
    name_basics_clean.writeTo("iceberg.silver.name_basics").overwrite()
    title_principals_clean.writeTo("iceberg.silver.title_principals").overwrite()
    
    logger.info("Silver tables created successfully")

def create_gold_tables(spark):
    """Create gold layer tables (business-ready aggregated data)."""
    logger.info("Creating gold layer tables...")
    
    # Gold: Business-ready aggregated data
    
    # Movies with ratings and metadata
    movies_gold = spark.sql("""
        SELECT 
            t.tconst,
            t.primaryTitle,
            t.originalTitle,
            t.startYear,
            t.runtimeMinutes,
            t.genres,
            tr.averageRating,
            tr.numVotes,
            CASE 
                WHEN tr.averageRating >= 8.0 THEN 'Excellent'
                WHEN tr.averageRating >= 7.0 THEN 'Good'
                WHEN tr.averageRating >= 6.0 THEN 'Average'
                ELSE 'Below Average'
            END as rating_category
        FROM iceberg.silver.title_basics t
        JOIN iceberg.silver.title_ratings tr ON t.tconst = tr.tconst
        WHERE t.titleType IN ('movie', 'tvMovie')
        ORDER BY tr.averageRating DESC, tr.numVotes DESC
    """)
    
    # Top actors by movie count
    top_actors = spark.sql("""
        SELECT 
            n.nconst,
            n.primaryName,
            n.birthYear,
            COUNT(DISTINCT tp.tconst) as movie_count,
            AVG(tr.averageRating) as avg_movie_rating
        FROM iceberg.silver.name_basics n
        JOIN iceberg.silver.title_principals tp ON n.nconst = tp.nconst
        JOIN iceberg.silver.title_basics t ON tp.tconst = t.tconst
        JOIN iceberg.silver.title_ratings tr ON t.tconst = tr.tconst
        WHERE tp.category IN ('actor', 'actress')
        GROUP BY n.nconst, n.primaryName, n.birthYear
        HAVING movie_count >= 5
        ORDER BY movie_count DESC, avg_movie_rating DESC
    """)
    
    # Genre popularity over time
    genre_popularity = spark.sql("""
        SELECT 
            explode(t.genres) as genre,
            t.startYear,
            COUNT(*) as movie_count,
            AVG(tr.averageRating) as avg_rating
        FROM iceberg.silver.title_basics t
        JOIN iceberg.silver.title_ratings tr ON t.tconst = tr.tconst
        WHERE t.startYear IS NOT NULL
        GROUP BY genre, t.startYear
        HAVING movie_count >= 5
        ORDER BY t.startYear DESC, movie_count DESC
    """)
    
    # Write gold tables
    movies_gold.writeTo("iceberg.gold.movies").overwrite()
    top_actors.writeTo("iceberg.gold.top_actors").overwrite()
    genre_popularity.writeTo("iceberg.gold.genre_popularity").overwrite()
    
    logger.info("Gold tables created successfully")

def main():
    """Main execution function."""
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Data path - can be overridden by environment variable
        data_path = os.getenv("IMDB_DATA_PATH", "/opt/data/raw")
        
        # Load IMDb data
        dataframes = load_imdb_data(spark, data_path)
        logger.info(f"Loaded {len(dataframes)} IMDb datasets")
        
        # Create data layers
        create_bronze_tables(spark, dataframes)
        create_silver_tables(spark, dataframes)
        create_gold_tables(spark)
        
        logger.info("IMDb ingestion completed successfully!")
        
        # Show some sample data
        logger.info("Sample data from gold.movies:")
        spark.sql("SELECT * FROM iceberg.gold.movies LIMIT 5").show()
        
    except Exception as e:
        logger.error(f"Error during IMDb ingestion: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
